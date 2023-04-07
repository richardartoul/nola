package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"golang.org/x/exp/slog"

	_ "net/http/pprof"

	"github.com/richardartoul/nola/cmdutils"
	"github.com/richardartoul/nola/virtual"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/registry/fdbregistry"
	"github.com/richardartoul/nola/virtual/registry/localregistry"

	"github.com/google/uuid"
)

var (
	port                        = flag.Int("port", 9090, "TCP port for HTTP server to bind")
	serverID                    = flag.String("serverID", uuid.New().String(), "ID to identify the server. Must be globally unique within the cluster")
	discoveryType               = flag.String("discoveryType", virtual.DiscoveryTypeLocalHost, "how the server should register itself with the discovery serice. Valid options: localhost|remote. Use localhost for local testing, use remote for multi-node setups")
	registryType                = flag.String("registryBackend", "memory", "backend to use for the Registry. Validation options: memory|foundationdb")
	foundationDBClusterFilePath = flag.String("foundationDBClusterFilePath", "", "path to use for the FoundationDB cluster file")
	logFormat                   = flag.String("logFormat", "text", "format to use for the logger. Currently it accepts 'text' for plain text and 'json' for printing in json format")
	logLevel                    = flag.String("logLevel", "debug", "level to use for the logger. Currently it accepts 'text' for plain text and 'json' for printing in json format")
)

func main() {
	flag.Parse()

	flag.VisitAll(func(f *flag.Flag) {
		fmt.Printf(" --%s=%s\n", f.Name, f.Value.String())
	})

	log, err := cmdutils.ParseLog(*logLevel, *logFormat)
	if err != nil {
		slog.Error("failed to parse log", slog.Any("error", err))
		return
	}

	var reg registry.Registry
	switch *registryType {
	case "memory":
		reg = localregistry.NewLocalRegistry()
	case "foundationdb":
		var err error
		reg, err = fdbregistry.NewFoundationDBRegistry(*foundationDBClusterFilePath)
		if err != nil {
			log.Error("error creating FoundationDB registry", slog.Any("error", err))
			return
		}
	default:
		log.Error("unknown registry type", slog.String("registryType", *registryType))
		return
	}

	client := virtual.NewHTTPClient()

	ctx, cc := context.WithTimeout(context.Background(), 10*time.Second)
	environment, err := virtual.NewEnvironment(ctx, log, *serverID, reg, client, virtual.EnvironmentOptions{
		Discovery: virtual.DiscoveryOptions{
			DiscoveryType: *discoveryType,
			Port:          *port,
		},
	})
	cc()
	if err != nil {
		log.Error("error creating environment", slog.Any("error", err))
		return
	}

	server := virtual.NewServer(reg, environment)

	log.Info("server listening", slog.Int("port", *port))

	if err := server.Start(*port); err != nil {
		log.Error(err.Error())
		return
	}
}
