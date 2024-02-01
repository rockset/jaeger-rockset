package storage

import (
	"io"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/rockset/rockset-go-client"

	rss "github.com/rockset/jaeger-rockset/storage/spanstore"
)

type Store struct {
	writer        spanstore.Writer
	reader        spanstore.Reader
	archiveWriter spanstore.Writer
	archiveReader spanstore.Reader
	closer        io.Closer
	setup         func() error
}

var (
	_ shared.StoragePlugin             = (*Store)(nil)
	_ shared.ArchiveStoragePlugin      = (*Store)(nil)
	_ shared.StreamingSpanWriterPlugin = (*Store)(nil)
	_ io.Closer                        = (*Store)(nil)
)

func New(logger hclog.Logger, rc *rockset.RockClient, config rss.Config) (*Store, error) {
	spanStore, err := rss.New(logger, rc, config)
	if err != nil {
		return nil, err
	}

	return &Store{
		writer:        spanStore,
		reader:        spanStore,
		closer:        spanStore,
		archiveWriter: spanStore,
		archiveReader: spanStore,
		setup:         spanStore.Setup,
	}, nil
}

func (s Store) StreamingSpanWriter() spanstore.Writer {
	return s.writer
}

func (s Store) ArchiveSpanReader() spanstore.Reader {
	return s.archiveReader
}

func (s Store) ArchiveSpanWriter() spanstore.Writer {
	return s.archiveWriter
}

func (s Store) SpanReader() spanstore.Reader {
	return s.reader
}

func (s Store) SpanWriter() spanstore.Writer {
	return s.writer
}

func (s Store) Close() error {
	return s.closer.Close()
}

func (s Store) DependencyReader() dependencystore.Reader {
	// TODO implement me
	panic("implement me")
}

func (s Store) Setup() error {
	return s.setup()
}
