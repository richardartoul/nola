package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/exp/slog"

	"net/http"
	_ "net/http/pprof"

	"github.com/richardartoul/nola/cmd/utils"
	"github.com/richardartoul/nola/inner"
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
	shutdownTimeout             = flag.Duration("shutdownTimeout", 0, "timeout until the server is forced to shutdown, without waiting actors and other components to close gracefully. By default is 0, which is infinite duration untill all actors are closed")
	logFormat                   = flag.String("logFormat", "text", "format to use for the logger. The formats it accepst are: 'text', 'json'")
	logLevel                    = flag.String("logLevel", "debug", "level to use for the logger. The levels it accepts are: 'info', 'debug', 'error', 'warn'")
	pprofEnabled                = flag.Bool("pprof", false, "enable pprod endpoint under '/debug/pprof/'")
	internalAddr                = flag.String("internalAddr", "0.0.0.0:9091", "internal server address, e.g. metrics and pprof")
)

func main() {
	flag.Parse()

	flag.VisitAll(func(f *flag.Flag) {
		fmt.Printf(" --%s=%s\n", f.Name, f.Value.String())
	})

	log, err := utils.ParseLog(*logLevel, *logFormat)
	if err != nil {
		slog.Error("failed to parse log", slog.Any("error", err))
		os.Exit(1)
	}

	log = log.With(slog.String("service", "nola"))

	var (
		reg         registry.Registry
		moduleStore registry.ModuleStore
	)
	switch *registryType {
	case "memory":
		reg = localregistry.NewLocalRegistry("test-server-id")
		// Local registry also implements ModuleStore for convenience.
		moduleStore = reg.(registry.ModuleStore)
	case "foundationdb":
		var err error
		reg, err = fdbregistry.NewFoundationDBRegistry("test-server-id", *foundationDBClusterFilePath)
		if err != nil {
			log.Error("error creating FoundationDB registry", slog.Any("error", err))
			os.Exit(1)
		}
		// FoundationDB registry also implements ModuleStore for convenience.
		moduleStore = reg.(registry.ModuleStore)
	default:
		log.Error("unknown registry type", slog.String("registryType", *registryType))
		os.Exit(1)
	}

	client := virtual.NewHTTPClient()

	ctx, cc := context.WithTimeout(context.Background(), 10*time.Second)
	environment, err := virtual.NewEnvironment(ctx, *serverID, reg, moduleStore, client, virtual.EnvironmentOptions{
		Discovery: virtual.DiscoveryOptions{
			DiscoveryType: *discoveryType,
			Port:          *port,
		},
		Logger: log,
	})
	cc()
	if err != nil {
		log.Error("error creating environment", slog.Any("error", err))
		os.Exit(1)
	}

	var server virtualServer = virtual.NewServer(moduleStore, environment)

	go func(server virtualServer) {
		sig := waitForSignal()
		log.Info("received signal", slog.Any("signal", sig))
		shutdown(log, server, *shutdownTimeout)
	}(server)

	// setup internal endpoint
	internalMux := http.NewServeMux()
	internalSrvr := http.Server{
		Addr:    *internalAddr,
		Handler: internalMux,
	}

	m, err := inner.NewMetrics(log)
	if err != nil {
		log.Error("failed to initialize metrics", slog.Any("error", err))
		os.Exit(1)
	}
	m.AttachMetrics(internalMux)

	if *pprofEnabled {
		inner.AttachPProf(internalMux)
		log.Info("pprof enabled", slog.String("addr", *internalAddr+"/debug/pprof"))
	}
	go func() {
		log.Info("internal server listening", slog.String("addr", *internalAddr))
		if err := internalSrvr.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("received error", slog.Any("error", err), slog.String("subService", "httpInternalServer"))
			shutdown(log, server, *shutdownTimeout)
		}
	}()

	log.Info("server listening", slog.Int("port", *port))
	if err := server.Start(*port); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("received error", slog.Any("error", err), slog.String("subService", "httpServer"))
		shutdown(log, server, *shutdownTimeout)
		os.Exit(1)
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

func shutdown(log *slog.Logger, server virtualServer, timeout time.Duration) {
	tStart := time.Now()
	log.Info("shutting down server with timeout...", slog.Duration("timeout", timeout))
	var (
		ctx = context.Background()
		cc  context.CancelFunc
	)
	if timeout > 0 { // by default there is no timeout for shutting down
		ctx, cc = context.WithTimeout(context.Background(), timeout)
		defer cc()
	}

	if err := server.Stop(ctx); err != nil {
		log.Error("failed to shut down server", slog.Any("error", err))
		return
	}
	log.Info("successfully shut down server", slog.Duration("duration", time.Since(tStart)))
}
