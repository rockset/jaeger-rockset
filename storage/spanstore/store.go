package spanstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/opentracing/opentracing-go"
	"github.com/rockset/rockset-go-client"
	rockerr "github.com/rockset/rockset-go-client/errors"
	"github.com/rockset/rockset-go-client/option"
	"github.com/rockset/rockset-go-client/writer"
)

type Config struct {
	Workspace     string `yaml:"workspace"`
	Spans         string `yaml:"spans"`
	Operations    string `yaml:"operations"`
	Workers       uint64 `yaml:"workers"`
	Create        bool   `yaml:"create"`
	RetentionSecs int64  `yaml:"retention_secs"`
}

const (
	DefaultWorkspace  = "tracing"
	DefaultSpans      = "spans"
	DefaultOperations = "operations"
	DefaultRetention  = 7 * 24 * 60 * 60 // 7 days
	DefaultWorkers    = 3
)

func (c *Config) SetDefaults() {
	if c.Workspace == "" {
		c.Workspace = DefaultWorkspace
	}
	if c.Spans == "" {
		c.Spans = DefaultSpans
	}
	if c.Operations == "" {
		c.Operations = DefaultOperations
	}
	if c.Workers == 0 {
		c.Workers = DefaultWorkers
	}
	if c.RetentionSecs == 0 {
		c.RetentionSecs = DefaultRetention
	}
}

type Store struct {
	ctx     context.Context
	logger  hclog.Logger
	rc      *rockset.RockClient
	writer  *writer.Writer
	config  Config
	counter int
}

func New(logger hclog.Logger, rc *rockset.RockClient, config Config) (*Store, error) {
	w, err := writer.New(writer.Config{
		FlushInterval: time.Second,
		ConversionFn:  writer.JSONConversion,
		Workers:       config.Workers,
	}, rc)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	go w.Run(ctx)

	return &Store{
		logger: logger,
		rc:     rc,
		writer: w,
		config: config,
	}, nil
}

func (s Store) Setup() error {
	if !s.config.Create {
		s.logger.Debug("skipping workspace and collection creation")
		return nil
	}
	s.logger.Debug("creating workspace and collections")

	ctx := context.Background()

	if err := s.createWorkspaceIfMissing(ctx, s.config.Workspace); err != nil {
		return err
	}

	if err := s.createCollectionIfMissing(ctx, s.config.Workspace, s.config.Spans); err != nil {
		return err
	}

	if err := s.createCollectionIfMissing(ctx, s.config.Workspace, s.config.Operations); err != nil {
		return err
	}

	return nil
}

func (s Store) createWorkspaceIfMissing(ctx context.Context, workspace string) error {
	_, err := s.rc.GetWorkspace(ctx, workspace)
	if err == nil {
		s.logger.Debug("workspace exists", "workspace", workspace)
		return nil
	}

	var re rockerr.Error
	if errors.As(err, &re) {
		if re.StatusCode == http.StatusNotFound {
			// collection is missing, create it
			if _, err = s.rc.CreateWorkspace(ctx, workspace); err != nil {
				return err
			}
			s.logger.Info("created workspace", "workspace", workspace)
		}
	}

	return err
}

func (s Store) createCollectionIfMissing(ctx context.Context, workspace, collection string) error {
	_, err := s.rc.GetCollection(ctx, workspace, collection)
	if err == nil {
		s.logger.Debug("collection exists", "workspace", workspace, "collection", collection)
		return nil
	}

	var re rockerr.Error
	if errors.As(err, &re) {
		if re.StatusCode == http.StatusNotFound {
			// collection is missing, create it
			if _, err = s.rc.CreateCollection(ctx, workspace, collection,
				option.WithCollectionRetentionSeconds(s.config.RetentionSecs)); err != nil {
				return err
			}
			s.logger.Info("created collection", "workspace", workspace, "collection", collection)
		}
	}

	return err
}

func (s Store) Close() error {
	s.writer.Stop()
	return nil
}

func (s Store) findTraces(ctx context.Context, ids []model.TraceID) ([]*model.Trace, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "findTraces")
	defer span.Finish()

	if len(ids) == 0 {
		return nil, nil
	}

	idList, err := traceIDs(ids)
	if err != nil {
		return nil, err
	}

	q := `SELECT * FROM %s.%s spans WHERE spans.trace_id IN (%s)`
	q2 := fmt.Sprintf(q, s.config.Workspace, s.config.Spans, idList)
	s.logger.Trace("findTraces query", "sql", q2)

	result, err := s.rc.Query(ctx, q2)
	if err != nil {
		return nil, err
	}
	s.logger.Debug("query result", "spans", len(result.Results))

	traces := map[model.TraceID]*model.Trace{}
	for _, row := range result.Results {
		span, err := toSpan(row)
		if err != nil {
			return nil, err
		}

		if _, found := traces[span.TraceID]; !found {
			traces[span.TraceID] = &model.Trace{}
		}

		traces[span.TraceID].Spans = append(traces[span.TraceID].Spans, &span)
	}

	ret := make([]*model.Trace, 0, len(traces))
	for _, trace := range traces {
		ret = append(ret, trace)
	}
	s.logger.Debug("result", "traces", len(ret))

	return ret, nil
}

// TODO: this is very naïve and must be improved to avoid SQL injection
func buildQuery(config Config, query *spanstore.TraceQueryParameters) string {
	// TODO implement tag filtering
	// {
	// 	Tags:map[sameplacetag1:sameplacevalue sameplacetag2:123 sameplacetag3:72.5 sameplacetag4:true]
	// }

	var q strings.Builder
	q.WriteString(fmt.Sprintf("SELECT DISTINCT trace_id, start_time FROM %s.%s spans", config.Workspace, config.Spans))

	q.WriteString(" WHERE ")
	if query.ServiceName != "" && query.OperationName != "" {
		q.WriteString(fmt.Sprintf("spans.process.service_name = '%s'", query.ServiceName))
		q.WriteString(" AND ")
		q.WriteString(fmt.Sprintf("spans.operation_name = '%s'", query.OperationName))
	} else if query.ServiceName != "" {
		q.WriteString(fmt.Sprintf("spans.process.service_name = '%s'", query.ServiceName))
	} else if query.OperationName != "" {
		q.WriteString(fmt.Sprintf("spans.operation_name = '%s'", query.OperationName))
	}

	// TODO create the collection with an ingest transformation to avoid parsing on each query
	q.WriteString(fmt.Sprintf(" AND PARSE_TIMESTAMP_ISO8601(spans.start_time) >= PARSE_TIMESTAMP_ISO8601('%s')",
		query.StartTimeMin.Format(time.RFC3339Nano)))
	if !query.StartTimeMax.IsZero() {
		q.WriteString(fmt.Sprintf(" AND PARSE_TIMESTAMP_ISO8601(spans.start_time) <= PARSE_TIMESTAMP_ISO8601('%s')",
			query.StartTimeMax.Format(time.RFC3339Nano)))
	}

	if query.DurationMin > 0 {
		q.WriteString(fmt.Sprintf(" AND spans.duration >= %d", query.DurationMin))
	}
	if query.DurationMax > 0 {
		q.WriteString(fmt.Sprintf(" AND spans.duration <= %d", query.DurationMax))
	}

	q.WriteString(" ORDER BY spans.start_time DESC, trace_id")

	if query.NumTraces > 0 {
		q.WriteString(fmt.Sprintf(" LIMIT %d", query.NumTraces))
	}

	return q.String()
}

// toSpan converts a map[string]any to a model.Span, which is an ugly hack, but works, and is ok for now
func toSpan(m map[string]any) (model.Span, error) {
	var span model.Span

	data, err := json.Marshal(m)
	if err != nil {
		return span, err
	}

	if err = json.Unmarshal(data, &span); err != nil {
		return span, err
	}

	return span, nil
}

func traceID(id model.TraceID) (string, error) {
	s, err := id.MarshalJSON() // returns the trace ID as a string surrounded by quotes
	if err != nil {
		return "", err
	}

	return strings.ReplaceAll(string(s), `"`, "'"), nil
}

func traceIDs(ids []model.TraceID) (string, error) {
	tids := make([]string, len(ids))
	for i, id := range ids {
		s, err := traceID(id)
		if err != nil {
			return "", err
		}
		tids[i] = s
	}

	return strings.Join(tids, ","), nil
}
