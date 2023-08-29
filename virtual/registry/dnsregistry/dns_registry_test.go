package dnsregistry

import (
	"context"
	"github.com/stretchr/testify/assert"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/stretchr/testify/require"
)

// TestDNSRegistrySimple is a simple test of the DNS registry. It tests that
// the background discovery loop works and that EnsureActivation uses
// consistent hashing to pick a server.
func TestDNSRegistrySimple(t *testing.T) {
	resolver := newConstResolver(nil)
	reg, err := NewDNSRegistryFromResolver(resolver, "test", DNSRegistryOptions{
		ResolveEvery: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	defer func() {
		if err := reg.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	_, err = reg.EnsureActivation(context.Background(), registry.EnsureActivationRequest{
		Namespace: "ns1",
		ActorID:   "a",
		ModuleID:  "test-module",
	})
	require.True(t, strings.Contains(err.Error(), "hashring is empty"))

	// Should be a no-op.
	_, err = reg.Heartbeat(context.Background(), "serverID", registry.HeartbeatState{})
	require.NoError(t, err)

	// Should always return a constant.
	for i := 0; i < 100; i++ {
		versionStamp, err := reg.GetVersionStamp(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(1), versionStamp)
	}

	resolver.setIPs([]registry.Address{
		{IP: net.ParseIP("127.0.0.1"), Port: 9090},
		{IP: net.ParseIP("127.0.0.2"), Port: 9090},
		{IP: net.ParseIP("127.0.0.3"), Port: 9090},
	})

	for {
		activations, err := reg.EnsureActivation(context.Background(), registry.EnsureActivationRequest{
			Namespace: "ns1",
			ActorID:   "a",
			ModuleID:  "test-module",
		})
		if err != nil {
			time.Sleep(time.Millisecond)
			continue
		}

		require.Equal(t, 1, len(activations.References))
		require.Equal(t, "a", activations.References[0].Virtual.ActorID)
		require.Equal(t, "test-module", activations.References[0].Virtual.ModuleID)
		require.Equal(t, "127.0.0.3:9090", activations.References[0].Physical.ServerState.Address)
		require.Equal(t, DNSServerID, activations.References[0].Physical.ServerID)
		require.Equal(t, DNSServerVersion, activations.References[0].Physical.ServerVersion)
		break
	}
}

// TestDNSRegistrySingleNode tests that its easy to use the DNSRegistry locally in tests and in
// single-node implementations without a lot of ceremony. In other words, it ensures that most
// applications can use the exact same code in production, as well as in their tests and that
// the DNS registry "just works" without having to setup custom fakes.
func TestDNSRegistrySingleNode(t *testing.T) {
	reg, err := NewDNSRegistry(Localhost, 9090, DNSRegistryOptions{
		ResolveEvery: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	defer func() {
		if err := reg.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	activations, err := reg.EnsureActivation(context.Background(), registry.EnsureActivationRequest{
		Namespace: "ns1",
		ActorID:   "a",
		ModuleID:  "test-module",
	})
	require.NoError(t, err)

	require.Equal(t, 1, len(activations.References))
	require.Equal(t, "a", activations.References[0].Virtual.ActorID)
	require.Equal(t, "test-module", activations.References[0].Virtual.ModuleID)
	require.Equal(t, "127.0.0.1:9090", activations.References[0].Physical.ServerState.Address)
	require.Equal(t, DNSServerID, activations.References[0].Physical.ServerID)
	require.Equal(t, DNSServerVersion, activations.References[0].Physical.ServerVersion)
}

func TestDNSRegistryTTL(t *testing.T) {
	reg, err := NewDNSRegistry(Localhost, 9090, DNSRegistryOptions{
		HeartbeatTTL: 25 * time.Millisecond,
	})
	require.NoError(t, err)
	assert.Equal(t, reg.HeartbeatTTL(), 25*time.Millisecond, "Expected heartbeat TTL to be 25ms")
}
