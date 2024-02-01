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
	q2 := fmt.Sprintf(q, s.config.Workspace, s.config.Operations)
	result, err := s.rc.Query(ctx, q2)
	if err != nil {
		return nil, err
	}

	services := make([]string, 0, len(result.Results))
	for _, row := range result.Results {
		if row["service"] != nil {
			services = append(services, row["service"].(string))
		}
	}
	span.SetTag("services", len(services))

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
    operations.service = '%s'
	AND operations.span_kind %s
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

	q2 := fmt.Sprintf(q, s.config.Workspace, s.config.Operations, query.ServiceName, kind)

	result, err := s.rc.Query(ctx, q2)
	if err != nil {
		return nil, err
	}

	operations := make([]spanstore.Operation, 0, len(result.Results))
	for _, row := range result.Results {
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

	q := fmt.Sprintf("SELECT * FROM %s.%s spans WHERE spans.trace_id = %s", s.config.Workspace, s.config.Spans, id)

	response, err := s.rc.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	s.logger.Debug("matching query", "spans", len(response.Results))

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

	for i, s := range spans {
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

	q := buildQuery(s.config, query)
	s.logger.Trace("query", "sql", q)

	p := paginate.New(s.rc)
	docs := make(chan map[string]any)

	var err error
	go func() {
		err = p.Query(ctx, docs, q)
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

	ids, err := s.FindTraceIDs(ctx, query)
	if err != nil {
		return nil, err
	}

	return s.findTraces(ctx, ids)
}
