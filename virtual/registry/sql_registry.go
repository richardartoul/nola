package registry

import (
	_ "github.com/lib/pq"
    _ "github.com/mattn/go-sqlite3"
)

// NewSQLRegistry creates a new SQL backed registry.
// Currently this only supports Postgres.
func NewSQLRegistry(dsn string) (Registry, error) {
	registry, err := newSQLKV("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return newValidatedRegistry(newKVRegistry(registry)), nil
}

func newTestSQLRegistry() (Registry, error) {
	registry, err := newSQLKV("sqlite3", "test.db")
	if err != nil {
		return nil, err
	}
	return newValidatedRegistry(newKVRegistry(registry)), nil
}
