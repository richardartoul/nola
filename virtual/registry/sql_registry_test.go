package registry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLRegistry(t *testing.T) {
	testAllCommon(t, func() Registry {
		registry, err := NewPostgresSQLRegistry("postgres://postgres:test1234@localhost:5432/postgres?sslmode=disable")
		require.NoError(t, err)

		registry.UnsafeWipeAll()

		return registry
	})
}
