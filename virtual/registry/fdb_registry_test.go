package registry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFDBRegistry(t *testing.T) {
	t.Skip("TODO: Only skip locally, but run in CI")

	testAllCommon(t, func() Registry {
		registry, err := NewFoundationDBRegistry("")
		require.NoError(t, err)

		registry.unsafeWipeAll()

		return registry
	})
}
