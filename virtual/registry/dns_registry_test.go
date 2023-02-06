package registry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDNSRegistry(t *testing.T) {
	_, err := NewDNSRegistry()
	require.NoError(t, err)
}
