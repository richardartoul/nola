package main

import (
	"flag"
	"log"

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
	registry := registry.NewLocal()
	environment, err := virtual.NewEnvironment(*serverID, registry)
	if err != nil {
		log.Fatal(err)
	}

	server := virtual.NewServer(registry, environment)

	log.Printf("listening on port: %d\n", *port)
	if err := server.Start(*port); err != nil {
		log.Fatal(err)
	}
}
