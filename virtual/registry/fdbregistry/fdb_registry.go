package fdbregistry

import "github.com/richardartoul/nola/virtual/registry"

// NewFoundationDBRegistry creates a new FoundationDB backed registry.
func NewFoundationDBRegistry(clusterFile string) (registry.Registry, error) {
	fdbKV, err := newFDBKV(clusterFile)
	if err != nil {
		return nil, err
	}
	return registry.NewKVRegistry(fdbKV), nil
}
