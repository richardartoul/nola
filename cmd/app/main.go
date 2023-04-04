package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "net/http/pprof"

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
	shutdownTimeout             = flag.Duration("shutdownTimeout", 10*time.Second, "timeout before forcing the server to shutdown, without waiting actors and other components to close gracefully")
)

func main() {
	flag.Parse()

	flag.VisitAll(func(f *flag.Flag) {
		fmt.Printf(" --%s=%s\n", f.Name, f.Value.String())
	})

	var reg registry.Registry
	switch *registryType {
	case "memory":
		reg = localregistry.NewLocalRegistry()
	case "foundationdb":
		var err error
		reg, err = fdbregistry.NewFoundationDBRegistry(*foundationDBClusterFilePath)
		if err != nil {
			log.Fatalf("error creating FoundationDB registry: %v\n", err)
		}
	default:
		log.Fatalf("unknown registry type: %v", *registryType)
	}

	client := virtual.NewHTTPClient()

	ctx, cc := context.WithTimeout(context.Background(), 10*time.Second)
	environment, err := virtual.NewEnvironment(ctx, *serverID, reg, client, virtual.EnvironmentOptions{
		Discovery: virtual.DiscoveryOptions{
			DiscoveryType: *discoveryType,
			Port:          *port,
		},
	})
	cc()
	if err != nil {
		log.Fatal(err)
	}

	var server Server = virtual.NewServer(reg, environment)

	log.Printf("listening on port: %d\n", *port)

	go func(server Server) {
		waitForSignal()
		shutdownServer(server, *shutdownTimeout)
	}(server)

	if err := server.Start(*port); err != nil {
		shutdownServer(server, *shutdownTimeout)
		log.Fatal(err)
	}
}

type Server interface {
	Start(int) error
	Stop(context.Context) error
}

func waitForSignal() {
	osSig := make(chan os.Signal, 1)
	signal.Notify(osSig, syscall.SIGTERM)
	signal.Notify(osSig, syscall.SIGINT)

	<-osSig
}

func shutdownServer(server Server, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Stop(ctx); err != nil {
		log.Printf("failed to gracefully shutdown server: %s", err.Error())
	}
}
