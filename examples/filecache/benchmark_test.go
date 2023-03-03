package filecache

import (
	"context"
	"log"
	"testing"

	"github.com/richardartoul/nola/virtual"
	"github.com/richardartoul/nola/virtual/registry/localregistry"
	"github.com/richardartoul/nola/virtual/types"
)

func BenchmarkFileCacheTest(t *testing.T) {
	registry := localregistry.NewLocalRegistry()

	env, err := virtual.NewEnvironment(
		context.Background(),
		"test-server-id", registry,
		virtual.NewHTTPClient(), virtual.EnvironmentOptions{
			GoModules: map[types.NamespacedIDNoType]virtual.Module{
				types.NewNamespacedIDNoType("example", "test-module"): &FileCacheModule{},
			},
		})
	if err != nil {
		log.Fatalf("error creating virtual environment: %v", err)
	}

	env.InvokeActorStream()
}
