package spanstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/opentracing/opentracing-go"
	"github.com/rockset/rockset-go-client/paginate"
)

func (s Store) GetServices(ctx context.Context) ([]string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "GetServices")
	defer span.Finish()

	q := `SELECT
    operations.service as service
FROM
    %s.%s operations
GROUP BY
    service
ORDER BY
    service
`
	sql := fmt.Sprintf(q, s.config.Workspace, s.config.Operations)
	s.logger.Info("GetServices query", "sql", sql)

	response, err := s.rc.Query(ctx, sql)
	if err != nil {
		return nil, err
	}

	services := make([]string, 0, len(response.Results))
	for _, row := range response.Results {
		if row["service"] != nil {
			services = append(services, row["service"].(string))
		}
	}
	span.SetTag("services", len(services))
	stats := response.GetStats()
	s.logger.Info("GetServices result", "services", len(services), "ms", stats.GetElapsedTimeMs())

	return services, nil
}

func (s Store) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "GetOperations")
	defer span.Finish()

	q := `SELECT
    operations.operation as operation,
    operations.span_kind as spankind
FROM
    %s.%s operations
WHERE
    operations.service = '%s' AND
    operations.span_kind %s
GROUP BY
    operation,
    spankind
ORDER BY
    operation,
    spankind`

	kind := "LIKE '%'"
	if query.SpanKind != "" {
		kind = fmt.Sprintf("= '%s'", query.SpanKind)
	}

	span.SetTag("service", query.ServiceName)
	span.SetTag("spankind", kind)

	sql := fmt.Sprintf(q, s.config.Workspace, s.config.Operations, query.ServiceName, kind)
	s.logger.Info("GetOperations query", "sql", sql)

	response, err := s.rc.Query(ctx, sql)
	if err != nil {
		return nil, err
	}

	operations := make([]spanstore.Operation, 0, len(response.Results))
	for _, row := range response.Results {
		if row["operation"] == nil {
			s.logger.Warn("ignoring", "row", row)
			continue
		}

		operations = append(operations, spanstore.Operation{
			Name:     row["operation"].(string),
			SpanKind: row["spankind"].(string),
		})
	}
	span.SetTag("operations", len(operations))
	stats := response.GetStats()
	s.logger.Info("GetOperations result", "operations", len(operations), "ms", stats.GetElapsedTimeMs())

	return operations, nil
}

func (s Store) GetTrace(ctx context.Context, tid model.TraceID) (*model.Trace, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "GetTrace")
	defer span.Finish()

	id, err := traceID(tid)
	if err != nil {
		return nil, err
	}
	span.SetTag("trace_id", id)

	sql := fmt.Sprintf("SELECT * FROM %s.%s spans WHERE spans.trace_id = %s", s.config.Workspace, s.config.Spans, id)
	s.logger.Info("GetTrace query", "sql", sql)

	response, err := s.rc.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	stats := response.GetStats()
	s.logger.Info("GetTrace result", "spans", len(response.Results), "ms", stats.GetElapsedTimeMs())

	if len(response.Results) == 0 {
		return nil, spanstore.ErrTraceNotFound
	}

	var spans []model.Span
	var trace model.Trace
	trace.Spans = make([]*model.Span, len(response.Results))

	data, err := json.Marshal(response.Results)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(data, &spans); err != nil {
		return nil, err
	}

	for i, span := range spans {
		s := span
		trace.Spans[i] = &s
	}

	return &trace, nil
}

func (s Store) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "FindTraceIDs")
	defer span.Finish()

	if query.StartTimeMin.IsZero() {
		return nil, errors.New("start time required")
	}

	sql := buildQuery(s.config, query)
	s.logger.Info("FindTraceIDs", "sql", sql)

	p := paginate.New(s.rc)
	docs := make(chan map[string]any)

	// TODO should this use a plain query instead of a paginated one, as we want to put a limit on the number of results?
	var err error
	go func() {
		err = p.Query(ctx, docs, sql)
	}()

	tids := make([]model.TraceID, 0, 100)
	for doc := range docs {
		if doc["trace_id"] == nil {
			s.logger.Warn("ignoring", "doc", doc)
			continue
		}

		id := doc["trace_id"].(string)
		var tid model.TraceID
		err = tid.UnmarshalJSON([]byte(`"` + id + `"`))
		if err != nil {
			s.logger.Error("failed to parse trace ID", "id", id, "err", err)
			continue
		}

		tids = append(tids, tid)
	}

	span.SetTag("trace_ids", len(tids))

	if err != nil {
		return nil, err
	}

	return tids, nil
}

func (s Store) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "FindTraces")
	defer span.Finish()

	s.logger.Info("FindTraces")
	ids, err := s.FindTraceIDs(ctx, query)
	if err != nil {
		return nil, err
	}

	return s.findTraces(ctx, ids)
}
