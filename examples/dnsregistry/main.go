package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/richardartoul/nola/cmdutils"
	"github.com/richardartoul/nola/virtual"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"
	"golang.org/x/exp/slog"
)

var (
	host      = flag.String("host", "localhost", "Hostname to perform DNS lookups against")
	port      = flag.Int("port", 9090, "TCP port for HTTP server to bind")
	logFormat = flag.String("logFormat", "text", "format to use for the logger. Currently it accepts 'text' for plain text and 'json' for printing in json format")
	logLevel  = flag.String("logLevel", "debug", "level to use for the logger. Currently it accepts 'text' for plain text and 'json' for printing in json format")
)

func main() {
	flag.Parse()

	if *host == "" {
		flag.Usage()
		slog.Error("host cannot be empty")
		return
	}

	log, err := cmdutils.ParseLog(*logLevel, *logFormat)
	if err != nil {
		slog.Error("failed to parse log", slog.Any("error", err))
		return
	}

	env, registry, err := virtual.NewDNSRegistryEnvironment(
		context.Background(), log, *host, *port, virtual.EnvironmentOptions{})
	if err != nil {
		log.Error("error creating virtual environment", slog.Any("error", err))
		return
	}

	err = env.RegisterGoModule(
		types.NewNamespacedIDNoType("example", "test-module"),
		&testModule{})
	if err != nil {
		log.Error("error registering Go module with virtual environment", slog.Any("error", err))
		return
	}

	go func() {
		for i := 0; ; i++ {
			time.Sleep(time.Second)

			ctx, cc := context.WithTimeout(context.Background(), 5*time.Second)
			actorID := fmt.Sprintf("actor-%d", i%10)
			resp, err := env.InvokeActor(
				ctx,
				"example", actorID, "test-module", "inc", nil,
				types.CreateIfNotExist{})
			cc()
			if err != nil {
				log.Info("error calling actor", slog.String("actor", actorID), slog.Any("error", err))
				continue
			}

			v, err := strconv.ParseInt(string(resp), 10, 64)
			if err != nil {
				panic(fmt.Sprintf(
					"actor %s returned unparseable response: %v",
					actorID, string(resp)))
			}
			log.Info("actor responsed", slog.String("actor", actorID), slog.Int64("response", v))
		}
	}()

	server := virtual.NewServer(registry, env)
	if err := server.Start(*port); err != nil {
		log.Error("error starting server", slog.Any("error", err))
		return
	}
}

type testModule struct {
}

func (tm testModule) Instantiate(
	ctx context.Context,
	reference types.ActorReferenceVirtual,
	payload []byte,
	host virtual.HostCapabilities,
) (virtual.Actor, error) {
	return &testActor{
		host: host,
	}, nil
}

func (tm testModule) Close(ctx context.Context) error {
	return nil
}

type testActor struct {
	host virtual.HostCapabilities

	count int
}

func (ta *testActor) Invoke(
	ctx context.Context,
	operation string,
	payload []byte,
	transaction registry.ActorKVTransaction,
) ([]byte, error) {
	switch operation {
	case wapcutils.StartupOperationName:
		return nil, nil
	case wapcutils.ShutdownOperationName:
		return nil, nil
	case "inc":
		ta.count++
		return []byte(strconv.Itoa(ta.count)), nil
	default:
		return nil, fmt.Errorf("testActor: unhandled operation: %s", operation)
	}
}

func (ta *testActor) Close(
	ctx context.Context,
) error {
	return nil
}
