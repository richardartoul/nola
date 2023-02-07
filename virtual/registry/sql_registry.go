package registry

// NewPostgresSQLRegistry creates a new Postgres backed registry.
func NewPostgresSQLRegistry(dsn string) (Registry, error) {
	registry, err := newPostgresSQLKV( dsn)
	if err != nil {
		return nil, err
	}
	return newValidatedRegistry(newKVRegistry(registry)), nil
}
