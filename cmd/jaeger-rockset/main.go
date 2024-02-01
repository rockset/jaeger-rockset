package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/rockset/rockset-go-client"
	"gopkg.in/yaml.v3"

	"github.com/rockset/jaeger-rockset/storage"
	"github.com/rockset/jaeger-rockset/storage/spanstore"
)

type Config struct {
	APIServer   string           `yaml:"apiserver"`
	APIKey      string           `yaml:"apikey"`
	StoreConfig spanstore.Config `yaml:"config"`
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "A path to the plugin's configuration file")
	flag.Parse()
	var cfg Config

	logger := hclog.New(&hclog.LoggerOptions{
		Name:       "jaeger-rockset",
		Level:      hclog.Trace,
		JSONFormat: true,
	})

	f, err := os.Open(configPath)
	if err != nil {
		logger.Error("failed to open config file", "err", err)
		os.Exit(1)
	}
	if err = yaml.NewDecoder(f).Decode(&cfg); err != nil {
		logger.Error("failed to decode config file", "err", err)
		os.Exit(1)
	}
	cfg.StoreConfig.SetDefaults()

	rc, err := rockset.NewClient(rockset.WithAPIServer(cfg.APIServer), rockset.WithAPIKey(cfg.APIKey))
	if err != nil {
		logger.Error("failed to create rockset client", "err", err)
		os.Exit(1)
	}
	log.Printf("connected to: %s", cfg.APIServer)

	plugin, err := storage.New(logger, rc, cfg.StoreConfig)
	if err != nil {
		logger.Error("failed to create plugin", "err", err)
		os.Exit(1)
	}
	logger.Info("store configuration", "workspace", cfg.StoreConfig.Workspace, "spans", cfg.StoreConfig.Spans,
		"operations", cfg.StoreConfig.Operations, "apiserver", cfg.APIServer,
		"create", cfg.StoreConfig.Create, "retention_secs", cfg.StoreConfig.RetentionSecs)

	if err = plugin.Setup(); err != nil {
		logger.Error("failed to setup plugin", "err", err)
		os.Exit(1)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, os.Kill)

	go func() {
		<-sigs
		// TODO how do we shut down the GRPC server?
		if err = plugin.Close(); err != nil {
			logger.Error("failed to close plugin", "err", err)
		}
		os.Exit(0)
	}()

	grpc.Serve(&shared.PluginServices{
		Store:        plugin,
		ArchiveStore: plugin,
	})
}
