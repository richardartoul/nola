package main

import (
	"log"

	_ "net/http/pprof"

	"github.com/richardartoul/nola/virtual"
	"github.com/richardartoul/nola/virtual/registry"
)

const (
	// TODO: Flags for all the different options.
	port = 9090
)

func main() {
	registry := registry.NewLocal()
	environment, err := virtual.NewEnvironment(registry)
	if err != nil {
		log.Fatal(err)
	}

	server := virtual.NewServer(registry, environment)

	log.Printf("listening on port: %d\n", port)
	if err := server.Start(port); err != nil {
		log.Fatal(err)
	}
}
