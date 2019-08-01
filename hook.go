package newrelicpg

import (
	"context"
	"strconv"
	"strings"

	"github.com/go-pg/pg"
	newrelic "github.com/newrelic/go-agent"
)

const (
	segmentKey = "newrelicSegment"
)

type NewRelicHook struct {
	agent newrelic.Application
	host  string
	port  string
	db    string
}

func NewHook(agent newrelic.Application, host, port, db string) *NewRelicHook {
	return &NewRelicHook{agent, host, port, db}
}

func (h *NewRelicHook) BeforeQuery(ev *pg.QueryEvent) {
	if h.agent == nil {
		return
	}
	txn := newrelic.FromContext(ev.Ctx)
	if txn == nil {
		return
	}
	query, err := ev.UnformattedQuery()
	if err != nil {
		return
	}
	operation, collection := parseQuery(query)
	segment := &newrelic.DatastoreSegment{
		StartTime:          newrelic.StartSegmentNow(txn.(newrelic.Transaction)),
		Product:            newrelic.DatastorePostgres,
		Collection:         collection,
		Operation:          operation,
		ParameterizedQuery: query,
		QueryParameters:    formatParams(ev.Params),
		Host:               h.host,
		PortPathOrID:       h.port,
		DatabaseName:       h.db,
	}
	ev.Ctx = context.WithValue(ev.Ctx, segmentKey, segment)
	return
}

func (h *NewRelicHook) AfterQuery(ev *pg.QueryEvent) {
	if h.agent == nil {
		return
	}
	segment := ev.Ctx.Value(segmentKey)
	if segment != nil {
		segment.(*newrelic.DatastoreSegment).End()
	}
	return
}

func parseQuery(query string) (operation string, collection string) {
	chunks := strings.Split(query, " ")
	if len(chunks) < 1 {
		return
	}
	operation = strings.ToUpper(chunks[0])
	switch operation {
	case "INSERT", "DELETE":
		if len(chunks) < 3 {
			return
		}
		collection = chunks[2]
	case "UPDATE":
		if len(chunks) < 2 {
			return
		}
		collection = chunks[1]
	case "SELECT":
		for i, chunk := range chunks {
			if strings.ToUpper(chunk) == "FROM" {
				if len(chunks) > i+1 {
					collection = chunks[i+1]
				}
				return
			}
		}
	}
	return
}

func formatParams(params []interface{}) map[string]interface{} {
	formatted := make(map[string]interface{}, len(params))
	for i, value := range params {
		formatted["$"+strconv.Itoa(i+1)] = value
	}
	return formatted
}
