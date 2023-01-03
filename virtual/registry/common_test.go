package registry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testAllCommon(t *testing.T, registryCtor func() Registry) {
	t.Run("simple", func(t *testing.T) {
		testRegistrySimple(t, registryCtor())
	})
	t.Run("service discovery and ensure activation", func(t *testing.T) {
		testRegistryServiceDiscoveryAndEnsureActivation(t, registryCtor())
	})

	t.Run("kv simple", func(t *testing.T) {
		testKVSimple(t, registryCtor())
	})
}

// testRegistrySimple is a basic smoke test that ensures we can register modules and create actors.
func testRegistrySimple(t *testing.T, registry Registry) {
	ctx := context.Background()

	// Create module.
	_, err := registry.RegisterModule(ctx, "ns1", "test-module", []byte("wasm"), ModuleOptions{})
	require.NoError(t, err)

	// Subsequent module for same namespace should fail.
	_, err = registry.RegisterModule(ctx, "ns1", "test-module", []byte("wasm"), ModuleOptions{})
	require.Error(t, err)

	// Succeeds with same module if different namespace.
	_, err = registry.RegisterModule(ctx, "ns2", "test-module", []byte("wasm"), ModuleOptions{})
	require.NoError(t, err)

	// Create actor fails for unknown module.
	_, err = registry.CreateActor(ctx, "ns1", "a", "unknown-module", ActorOptions{})
	require.Error(t, err)

	// Succeeds for known module.
	_, err = registry.CreateActor(ctx, "ns1", "a", "test-module", ActorOptions{})
	require.NoError(t, err)

	// Fails to create duplicate actor in same namespace.
	_, err = registry.CreateActor(ctx, "ns1", "a", "test-module", ActorOptions{})
	require.Error(t, err)

	// Allows actors with same ID in different namespaces.
	_, err = registry.CreateActor(ctx, "ns2", "a", "test-module", ActorOptions{})
	require.NoError(t, err)
}

// testRegistryServiceDiscoveryAndEnsureActivation tests the combination of the
// service discovery system and EnsureActivation() method to ensure we can:
//  1. Register servers.
//  2. Load balance across servers.
//  3. Remember which server an actor activation is currently assigned to.
//  4. Detect dead servers and reactive actors elsewhere.
func testRegistryServiceDiscoveryAndEnsureActivation(t *testing.T, registry Registry) {
	ctx := context.Background()

	// Create module and actor to experiment with.
	_, err := registry.RegisterModule(ctx, "ns1", "test-module", []byte("wasm"), ModuleOptions{})
	require.NoError(t, err)

	_, err = registry.CreateActor(ctx, "ns1", "a", "test-module", ActorOptions{})
	require.NoError(t, err)

	// Should fail because there are no servers available to activate on.
	_, err = registry.EnsureActivation(ctx, "ns1", "a")
	require.Error(t, err)

	err = registry.Heartbeat(ctx, "server1", HeartbeatState{
		NumActivatedActors: 100,
		Address:            "server1_address",
	})
	require.NoError(t, err)

	// Should succeed now that we have a server to activate on.
	activations, err := registry.EnsureActivation(ctx, "ns1", "a")
	require.NoError(t, err)
	require.Equal(t, 1, len(activations))
	require.Equal(t, "server1", activations[0].ServerID())
	require.Equal(t, "server1_address", activations[0].Address())
	require.Equal(t, "ns1", activations[0].Namespace())
	require.Equal(t, "ns1", activations[0].ModuleID().Namespace)
	require.Equal(t, "test-module", activations[0].ModuleID().ID)
	require.Equal(t, "ns1", activations[0].ActorID().Namespace)
	require.Equal(t, "a", activations[0].ActorID().ID)
	require.Equal(t, uint64(0), activations[0].Generation())

	// Ensure we get back all the same information but with the generation
	// bumped now.
	require.NoError(t, registry.IncGeneration(ctx, "ns1", "a"))
	activations, err = registry.EnsureActivation(ctx, "ns1", "a")
	require.NoError(t, err)
	require.Equal(t, 1, len(activations))
	require.Equal(t, "server1", activations[0].ServerID())
	require.Equal(t, "server1_address", activations[0].Address())
	require.Equal(t, "ns1", activations[0].Namespace())
	require.Equal(t, "ns1", activations[0].ModuleID().Namespace)
	require.Equal(t, "test-module", activations[0].ModuleID().ID)
	require.Equal(t, "ns1", activations[0].ActorID().Namespace)
	require.Equal(t, "a", activations[0].ActorID().ID)
	require.Equal(t, uint64(1), activations[0].Generation())

	// Add another server, this one with no existing activations.
	err = registry.Heartbeat(ctx, "server2", HeartbeatState{
		NumActivatedActors: 0,
		Address:            "server2_address",
	})
	require.NoError(t, err)

	// Keep checking the activation of the existing actor, it should remain sticky to
	// server 1.
	for i := 0; i < 100; i++ {
		// Should succeed now that we have a server to activate on.
		activations, err := registry.EnsureActivation(ctx, "ns1", "a")
		require.NoError(t, err)
		require.Equal(t, 1, len(activations))
		require.Equal(t, "server1", activations[0].ServerID())
		require.Equal(t, "server1_address", activations[0].Address())
		require.Equal(t, "ns1", activations[0].Namespace())
		require.Equal(t, "ns1", activations[0].ModuleID().Namespace)
		require.Equal(t, "test-module", activations[0].ModuleID().ID)
		require.Equal(t, "ns1", activations[0].ActorID().Namespace)
		require.Equal(t, "a", activations[0].ActorID().ID)
	}

	// Next 100 activations should all go to server2 for balancing purposes.
	for i := 0; i < 100; i++ {
		actorID := fmt.Sprintf("0-%d", i)
		_, err = registry.CreateActor(ctx, "ns1", actorID, "test-module", ActorOptions{})
		require.NoError(t, err)

		activations, err = registry.EnsureActivation(ctx, "ns1", actorID)
		require.NoError(t, err)
		require.Equal(t, 1, len(activations))
		require.Equal(t, "server2", activations[0].ServerID())

		err = registry.Heartbeat(ctx, "server2", HeartbeatState{
			NumActivatedActors: i + 1,
			Address:            "server2_address",
		})
		require.NoError(t, err)
	}

	// Subsequent activations should load balance.
	var lastServerID string
	for i := 0; i < 100; i++ {
		actorID := fmt.Sprintf("1-%d", i)
		_, err = registry.CreateActor(ctx, "ns1", actorID, "test-module", ActorOptions{})
		require.NoError(t, err)

		activations, err = registry.EnsureActivation(ctx, "ns1", actorID)
		require.NoError(t, err)
		require.Equal(t, 1, len(activations))

		if lastServerID == "" {
		} else if lastServerID == "server1" {
			require.Equal(t, "server2", activations[0].ServerID())
		} else {
			require.Equal(t, "server1", activations[0].ServerID())
		}
		err = registry.Heartbeat(ctx, activations[0].ServerID(), HeartbeatState{
			NumActivatedActors: 100 + i + 1,
			Address:            fmt.Sprintf("%s_address", activations[0].ServerID()),
		})
		require.NoError(t, err)
		lastServerID = activations[0].ServerID()
	}

	// Wait for server1's heartbeat to expire.
	//
	// TODO: Sleeps in tests are bad, but I'm lazy to inject a clock right now and deal
	//       with all of that.
	time.Sleep(MaxHeartbeatDelay + time.Second)

	// Heartbeat server2. After this, the Registry should only consider server2 to be alive.
	err = registry.Heartbeat(ctx, "server2", HeartbeatState{
		NumActivatedActors: 9999999,
		Address:            "server2_address",
	})
	require.NoError(t, err)

	// Even though server2's NumActivatedActors value is very high, all activations will go to
	// server2 because its the only one available.
	for i := 0; i < 100; i++ {
		actorID := fmt.Sprintf("2-%d", i)
		_, err = registry.CreateActor(ctx, "ns1", actorID, "test-module", ActorOptions{})
		require.NoError(t, err)

		activations, err = registry.EnsureActivation(ctx, "ns1", actorID)
		require.NoError(t, err)
		require.Equal(t, 1, len(activations))
		require.Equal(t, "server2", activations[0].ServerID())
	}
}

func testKVSimple(t *testing.T, registry Registry) {
	ctx := context.Background()

	// Neither PUT nor GET should work until an actor exists.
	err := registry.ActorKVPut(ctx, "ns1", "a", []byte("key1"), []byte("hello world"))
	require.Error(t, err)
	_, _, err = registry.ActorKVGet(ctx, "ns1", "a", []byte("key1"))
	require.Error(t, err)

	// Create the module/actor.
	_, err = registry.RegisterModule(ctx, "ns1", "test-module", []byte("wasm"), ModuleOptions{})
	require.NoError(t, err)
	_, err = registry.CreateActor(ctx, "ns1", "a", "test-module", ActorOptions{})
	require.NoError(t, err)

	// PUT/GET should work now.
	_, ok, err := registry.ActorKVGet(ctx, "ns1", "a", []byte("key1"))
	require.NoError(t, err)
	// key1 should not exist yet.
	require.False(t, ok)

	// Store key1 now. Subsequent GET should work.
	err = registry.ActorKVPut(ctx, "ns1", "a", []byte("key1"), []byte("hello world"))
	require.NoError(t, err)

	val, ok, err := registry.ActorKVGet(ctx, "ns1", "a", []byte("key1"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("hello world"), val)
}
