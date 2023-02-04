package registry

import (
	"os"
	"path/filepath"
	"testing"

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

func newTestSQLRegistry(t *testing.T) (Registry, error) {
	dir, err := os.MkdirTemp("", "sqlite-test*")
	if err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dir, "test.db")

	t.Log(dbPath)

	registry, err := newSQLKV("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	return newValidatedRegistry(newKVRegistry(registry)), nil
}
