package main

import (
	"context"
	"flag"
	"log"
	"time"

	_ "net/http/pprof"

	"github.com/google/uuid"

	"github.com/richardartoul/nola/virtual"
	"github.com/richardartoul/nola/virtual/registry"
)

var (
	port                        = flag.Int("port", 9090, "TCP port for HTTP server to bind")
	serverID                    = flag.String("serverID", uuid.New().String(), "ID to identify the server. Must be globally unique within the cluster.")
	discoveryType               = flag.String("discoveryType", virtual.DiscoveryTypeLocal, "how the server should register itself with the discovery serice. Valid options: local|remote. Use local for local testing, use remote for multi-server setups.")
	registryType                = flag.String("registryBackend", "local", "backend to use for the Registry. Validation options: local|foundationdb.")
	foundationDBClusterFilePath = flag.String("foundationDBClusterFilePath", "", "path to use for the FoundationDB cluster file")
)

func main() {
	var reg registry.Registry
	switch *registryType {
	case "local":
		reg = registry.NewLocalRegistry()
	case "foundationdb":
		var err error
		reg, err = registry.NewFoundationDBRegistry("")
		if err != nil {
			log.Fatalf("error creating FoundationDB registry: %w\n", err)
		}
	}

	ctx, cc := context.WithTimeout(context.Background(), 10*time.Second)
	environment, err := virtual.NewEnvironment(ctx, *serverID, reg, nil, virtual.EnvironmentOptions{
		Discovery: virtual.DiscoveryOptions{
			DiscoveryType: *discoveryType,
			Port:          *port,
		},
	})
	cc()
	if err != nil {
		log.Fatal(err)
	}

	server := virtual.NewServer(reg, environment)

	log.Printf("listening on port: %d\n", *port)

	if err := server.Start(*port); err != nil {
		log.Fatal(err)
	}
}
