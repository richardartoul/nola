package registry

// NewFoundationDBRegistry creates a new FoundationDB backed registry.
func NewFoundationDBRegistry(clusterFile string) (Registry, error) {
	registry, err := newFDBKV(clusterFile)
	if err != nil {
		return nil, err
	}
	return newValidatedRegistry(newKVRegistry(registry)), nil
}
