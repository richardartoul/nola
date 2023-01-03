package registry

import (
	"testing"
)

func TestLocalRegistrySimple(t *testing.T) {
	testRegistrySimple(t, NewLocal())
}

func TestLocalRegistryServiceDiscoveryAndEnsureActivation(t *testing.T) {
	testRegistryServiceDiscoveryAndEnsureActivation(t, NewLocal())
}

func TestLocalKVSimple(t *testing.T) {
	testKVSimple(t, NewLocal())
}
