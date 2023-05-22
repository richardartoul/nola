package localregistry

import "github.com/richardartoul/nola/virtual/registry"

// NewLocalRegistry creates a new local (in-memory) registry. It is primarily used for
// tests and simple benchmarking.
func NewLocalRegistry(selfID string) registry.Registry {
	return NewLocalRegistryWithOptions(selfID, registry.KVRegistryOptions{})
}

// NewLocalRegistryWithOptions is the same as NewLocalRegistry() except it allows the
// caller to provide KV registry options instead of relying on all the defaults.
func NewLocalRegistryWithOptions(
	selfID string,
	opts registry.KVRegistryOptions,
) registry.Registry {
	return registry.NewKVRegistry(selfID, newLocalKV(), opts)
}
