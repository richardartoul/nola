package registry

import (
    "testing"

	"github.com/stretchr/testify/require"
)


func TestSQLRegistry(t *testing.T) {
	testAllCommon(t, func() Registry {
		registry, err := newTestSQLRegistry()
		require.NoError(t, err)

		registry.UnsafeWipeAll()

		return registry
	})
}

