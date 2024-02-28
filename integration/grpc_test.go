package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/integration"
	"github.com/rockset/rockset-go-client"
	"github.com/rockset/rockset-go-client/wait"
	"github.com/stretchr/testify/require"

	"github.com/rockset/jaeger-rockset/storage"
	rss "github.com/rockset/jaeger-rockset/storage/spanstore"
)

func skipIfNotSet(t *testing.T, env string) {
	if _, found := os.LookupEnv(env); !found {
		t.Skipf("%s not set", env)
	}
}

func TestStorageIntegration(t *testing.T) {
	skipIfNotSet(t, "ROCKSET_APIKEY")
	skipIfNotSet(t, "ROCKSET_APISERVER")

	rc, err := rockset.NewClient()
	require.NoError(t, err)

	logger := hclog.New(&hclog.LoggerOptions{
		Name:       "test",
		Level:      hclog.Trace,
		JSONFormat: true,
	})
	cfg := rss.Config{
		Workspace:  "test",
		Spans:      "spans",
		Operations: "operations",
	}
	store, err := storage.New(logger, rc, cfg)
	require.NoError(t, err)

	si := integration.StorageIntegration{
		SpanReader: store.SpanReader(),
		SpanWriter: store.SpanWriter(),
		CleanUp:    deleteEverything(t, rc, cfg),
		Refresh:    countSpans(t, rc, cfg),
		SkipList:   []string{},
	}

	si.IntegrationTestAll(t)
}

func countSpans(t *testing.T, rc *rockset.RockClient, cfg rss.Config) func() error {
	return func() error {
		ctx := context.TODO()

		for _, coll := range []string{cfg.Spans, cfg.Operations} {
			res, err := rc.Query(ctx, fmt.Sprintf("SELECT _id FROM %s.%s", cfg.Workspace, coll))
			if err != nil {
				return err
			}
			t.Logf("refresh: %d %s", len(res.Results), coll)
		}

		return nil
	}
}

func deleteEverything(t *testing.T, rc *rockset.RockClient, cfg rss.Config) func() error {
	return func() error {
		ctx := context.TODO()
		t.Logf("deleting everything")

		for _, coll := range []string{cfg.Spans, cfg.Operations} {
			res, err := rc.Query(ctx, fmt.Sprintf("SELECT _id FROM %s.%s", cfg.Workspace, coll))
			if err != nil {
				return err
			}

			ids := make([]string, len(res.Results))
			for i, r := range res.Results {
				if i > 10000 {
					break
				}
				ids[i] = r["_id"].(string)
			}

			response, err := rc.DeleteDocumentsWithOffset(ctx, cfg.Workspace, coll, ids)
			if err != nil {
				return err
			}

			for _, d := range response.GetData() {
				if d.GetStatus() != "DELETED" {
					t.Logf("failed to delete %s: %s", d.GetId(), d.GetStatus())
				}
			}
			t.Logf("%s: deleted %d documents", coll, len(response.GetData()))

			// wait for the deletion to have propagated
			w := wait.New(rc)
			if err = w.UntilQueryable(ctx, cfg.Workspace, coll, []string{response.GetLastOffset()}); err != nil {
				return err
			}
		}

		return nil
	}
}
