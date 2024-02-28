package spanstore

import (
	"context"
	"strconv"

	"github.com/jaegertracing/jaeger/model"
	"github.com/rockset/rockset-go-client/writer"
)

const unspecified = "unspecified"

type Span struct {
	model.Span
	KV map[string]string `json:"kv"`
}

func extractKeyAndValue(tag model.KeyValue) (k, v string) {
	var s string

	switch tag.VType {
	case model.StringType:
		s = tag.VStr
	case model.BoolType:
		s = strconv.FormatBool(tag.VBool)
	case model.Int64Type:
		s = strconv.FormatInt(tag.VInt64, 10)
	case model.Float64Type:
		s = strconv.FormatFloat(tag.VFloat64, 'f', -1, 64)
	case model.BinaryType:
		s = string(tag.VBinary)
	}

	return tag.Key, s
}

func (s Store) WriteSpan(_ context.Context, span *model.Span) error {
	// to speed up queries we convert tags & process tags to a single map of string keys and string values,
	// as that is what we get from the web ui when someone is searching for a trace,
	// which makes the query much faster as we index the keys and values.
	sp := Span{
		Span: *span,
		KV:   make(map[string]string),
	}

	for _, tag := range span.Tags {
		k, v := extractKeyAndValue(tag)
		sp.KV[k] = v
	}
	for _, tag := range span.Process.Tags {
		k, v := extractKeyAndValue(tag)
		sp.KV[k] = v
	}

	s.writer.C() <- writer.Request{
		Workspace:  s.config.Workspace,
		Collection: s.config.Spans,
		Data:       sp,
	}

	// TODO we should batch these updates, to reduce the number of writes
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
