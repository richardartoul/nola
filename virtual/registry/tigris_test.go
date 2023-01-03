package registry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const localURL = "localhost:8081"

func TestTigriseRegistrySimple(t *testing.T) {
	tigris, err := NewTigris(localURL)
	require.NoError(t, err)
	testRegistrySimple(t, tigris)
}

func TestTigrisRegistryServiceDiscoveryAndEnsureActivation(t *testing.T) {
	tigris, err := NewTigris(localURL)
	require.NoError(t, err)
	testRegistryServiceDiscoveryAndEnsureActivation(t, tigris)
}

func TestTigrisKVSimple(t *testing.T) {
	tigris, err := NewTigris(localURL)
	require.NoError(t, err)
	testKVSimple(t, tigris)
}
