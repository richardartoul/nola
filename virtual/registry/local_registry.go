package registry

// NewLocalRegistry creates a new local (in-memory) registry. It is primarily used for
// tests and simple benchmarking.
func NewLocalRegistry() Registry {
	return newValidatedRegistry(newKVRegistry(newLocalKV()))
}
