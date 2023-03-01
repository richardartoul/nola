package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/richardartoul/nola/virtual"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/registry/dnsregistry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"
)

var (
	host = flag.String("host", "localhost", "Hostname to perform DNS lookups against")
	port = flag.Int("port", 9090, "TCP port for HTTP server to bind")
)

func main() {
	flag.Parse()

	if *host == "" {
		flag.Usage()
		log.Fatalf("host cannot be empty")
	}

	registry, err := dnsregistry.NewDNSRegistry(
		dnsregistry.NewDNSResolver(), *host, *port, dnsregistry.DNSRegistryOptions{})
	if err != nil {
		log.Fatalf(
			"error creating DNS registry: %v, make sure /etc/hosts contains an entry for host: %s",
			err, *host)
	}

	env, err := virtual.NewEnvironment(
		context.Background(),
		dnsregistry.DNSServerID, registry,
		virtual.NewHTTPClient(), virtual.EnvironmentOptions{
			GoModules: map[types.NamespacedIDNoType]virtual.Module{
				types.NewNamespacedIDNoType("example", "test-module"): &testModule{},
			},
		})
	if err != nil {
		log.Fatalf("error creating virtual environment: %v", err)
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
				log.Printf("error calling actor: %s, err: %v", actorID, err)
				continue
			}

			v, err := strconv.ParseInt(string(resp), 10, 64)
			if err != nil {
				panic(fmt.Sprintf(
					"actor %s returned unparseable response: %v",
					actorID, string(resp)))
			}
			log.Printf("actor: %s response: %d", actorID, v)
		}
	}()

	server := virtual.NewServer(registry, env)
	if err := server.Start(*port); err != nil {
		log.Fatalf("error starting server: %v", err)
	}
}

type testModule struct {
}

func (tm testModule) Instantiate(
	ctx context.Context,
	id string,
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