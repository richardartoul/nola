package fdbregistry

import "github.com/richardartoul/nola/virtual/registry"

// NewFoundationDBRegistry creates a new FoundationDB backed registry.
func NewFoundationDBRegistry(clusterFile string) (registry.Registry, error) {
	fdbKV, err := newFDBKV(clusterFile)
	if err != nil {
		return nil, err
	}
	return registry.NewKVRegistry(fdbKV, registry.KVRegistryOptions{
		// FDB may struggle to make progress on certain workloads if high
		// conflict operations are enabled because they'll cause excessive
		// transaction retries / failures.
		DisableHighConflictOperations: true,
	}), nil
}
