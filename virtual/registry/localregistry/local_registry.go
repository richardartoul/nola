package localregistry

import "github.com/richardartoul/nola/virtual/registry"

// NewLocalRegistry creates a new local (in-memory) registry. It is primarily used for
// tests and simple benchmarking.
func NewLocalRegistry() registry.Registry {
	return registry.NewKVRegistry(newLocalKV())
}
