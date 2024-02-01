package spanstore

import (
	"context"

	"github.com/jaegertracing/jaeger/model"
	"github.com/rockset/rockset-go-client/writer"
)

const unspecified = "unspecified"

func (s Store) WriteSpan(_ context.Context, span *model.Span) error {
	s.writer.C() <- writer.Request{
		Workspace:  s.config.Workspace,
		Collection: s.config.Spans,
		Data:       *span,
	}

	kind := unspecified
	for _, tag := range span.Tags {
		if tag.Key == "span.kind" {
			kind = tag.VStr
		}
	}

	s.writer.C() <- writer.Request{
		Workspace:  s.config.Workspace,
		Collection: s.config.Operations,
		Data: Operation{
			ID:        span.Process.ServiceName + ":" + span.OperationName,
			Service:   span.Process.ServiceName,
			Operation: span.OperationName,
			Kind:      kind,
		},
	}

	return nil
}

type Operation struct {
	ID        string `json:"_id"`
	Service   string `json:"service"`
	Operation string `json:"operation"`
	Kind      string `json:"span_kind"`
}
