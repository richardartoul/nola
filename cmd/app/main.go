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
	port     = flag.Int("port", 9090, "TCP port for HTTP server to bind")
	serverID = flag.String("serverID", uuid.New().String(), "ID to identify the server. Must be globally unique within the cluster.")
)

func main() {
	registry := registry.NewLocalRegistry()

	ctx, cc := context.WithTimeout(context.Background(), 10*time.Second)
	environment, err := virtual.NewEnvironment(ctx, *serverID, registry)
	cc()
	if err != nil {
		log.Fatal(err)
	}

	server := virtual.NewServer(registry, environment)

	log.Printf("listening on port: %d\n", *port)
	if err := server.Start(*port); err != nil {
		log.Fatal(err)
	}
}
