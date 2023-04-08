package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"net/http"
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
	shutdownTimeout             = flag.Duration("shutdownTimeout", 0, "timeout before forcing the server to shutdown, without waiting actors and other components to close gracefully. By default is 0, which is infinite duration untill all actors are closed")
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

	var server virtualServer = virtual.NewServer(reg, environment)

	log.Printf("listening on port: %d\n", *port)

	go func(server virtualServer) {
		sig := waitForSignal()
		log.Printf("received signal: %s", sig.String())
		shutdown(server, *shutdownTimeout)
	}(server)

	if err := server.Start(*port); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("received error: %s", err.Error())
		shutdown(server, *shutdownTimeout)
		log.Fatal(err)
	}
}

type virtualServer interface {
	Start(int) error
	Stop(context.Context) error
}

func waitForSignal() os.Signal {
	osSig := make(chan os.Signal, 1)
	signal.Notify(osSig, syscall.SIGTERM)
	signal.Notify(osSig, syscall.SIGINT)

	// wait for a signal to be received
	return <-osSig
}

func shutdown(server virtualServer, timeout time.Duration) {
	log.Printf("shutting down server with timeout (%s)...", timeout.String())
	var (
		ctx = context.Background()
		cc  context.CancelFunc
	)
	if timeout > 0 { // by default there is no timeout for shutting down
		ctx, cc = context.WithTimeout(context.Background(), timeout)
		defer cc()
	}

	if err := server.Stop(ctx); err != nil {
		log.Printf("failed to shut down server: %s", err.Error())
		return
	}
	log.Printf("successfully shut down server (%s)...", timeout.String())
}
