package registry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TODO: Add some concurrency tests.

// This is called from the specific registry implementation subpackages like
// fdbregistry, localregistry, dnsregistry, etc.
func TestAllCommon(t *testing.T, registryCtor func() Registry) {
	t.Run("service discovery and ensure activation", func(t *testing.T) {
		testRegistryServiceDiscoveryAndEnsureActivation(t, registryCtor())
	})
}

// testRegistryServiceDiscoveryAndEnsureActivation tests the combination of the
// service discovery system and EnsureActivation() method to ensure we can:
//  1. Register servers.
//  2. Load balance across servers.
//  3. Remember which server an actor activation is currently assigned to.
//  4. Detect dead servers and reactive actors elsewhere.
func testRegistryServiceDiscoveryAndEnsureActivation(t *testing.T, registry Registry) {
	ctx := context.Background()
	defer registry.Close(ctx)

	// Should fail because there are no servers available to activate on.
	_, err := registry.EnsureActivation(ctx, EnsureActivationRequest{
		Namespace: "ns1",
		ActorID:   "a",
		ModuleID:  "test-module1",
	})
	require.Error(t, err)
	require.False(t, IsActorDoesNotExistErr(err))

	var heartbeatResult HeartbeatResult
	for i := 0; i < 5; i++ {
		// Heartbeat 5 times because some registry implementations (like the
		// LeaderRegistry) require multiple successful heartbeats from at least
		// 1 server before any actors can be placed.
		heartbeatResult, err = registry.Heartbeat(ctx, "server1", HeartbeatState{
			NumActivatedActors: 10,
			Address:            "server1_address",
		})
		require.NoError(t, err)
		require.True(t, heartbeatResult.VersionStamp > 0)
		require.Equal(t, HeartbeatTTL.Microseconds(), heartbeatResult.HeartbeatTTL)
	}

	// Should succeed now that we have a server to activate on.
	activations, err := registry.EnsureActivation(ctx, EnsureActivationRequest{
		Namespace:     "ns1",
		ActorID:       "a",
		ModuleID:      "test-module1",
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(activations.References))
	require.Equal(t, "server1", activations.References[0].ServerID())
	require.Equal(t, "server1_address", activations.References[0].ServerState().Address())
	require.Equal(t, "ns1", activations.References[0].Namespace())
	require.Equal(t, "ns1", activations.References[0].ModuleID().Namespace)
	require.Equal(t, "test-module1", activations.References[0].ModuleID().ID)
	require.Equal(t, "ns1", activations.References[0].ActorID().Namespace)
	require.Equal(t, "a", activations.References[0].ActorID().ID)
	require.Equal(t, uint64(1), activations.References[0].Generation())
	require.True(t, activations.VersionStamp > 0)
	prevVS := activations.VersionStamp

	activations, err = registry.EnsureActivation(ctx, EnsureActivationRequest{
		Namespace:     "ns1",
		ActorID:       "a",
		ModuleID:      "test-module1",
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(activations.References))
	require.Equal(t, "server1", activations.References[0].ServerID())
	require.Equal(t, "server1_address", activations.References[0].ServerState().Address())
	require.Equal(t, "ns1", activations.References[0].Namespace())
	require.Equal(t, "ns1", activations.References[0].ModuleID().Namespace)
	require.Equal(t, "test-module1", activations.References[0].ModuleID().ID)
	require.Equal(t, "ns1", activations.References[0].ActorID().Namespace)
	require.Equal(t, "a", activations.References[0].ActorID().ID)
	require.Equal(t, uint64(1), activations.References[0].Generation())
	require.True(t, activations.VersionStamp > prevVS)
	prevVS = activations.VersionStamp

	// Add another server, this one with no existing activations.
	newHeartbeatResult, err := registry.Heartbeat(ctx, "server2", HeartbeatState{
		NumActivatedActors: 0,
		Address:            "server2_address",
	})
	require.NoError(t, err)
	require.True(t, newHeartbeatResult.VersionStamp > heartbeatResult.VersionStamp)
	require.Equal(t, newHeartbeatResult.HeartbeatTTL, heartbeatResult.HeartbeatTTL)

	// Keep checking the activation of the existing actor, it should remain sticky to
	// server 1.
	for i := 0; i < 10; i++ {
		activations, err := registry.EnsureActivation(ctx, EnsureActivationRequest{
			Namespace:     "ns1",
			ActorID:       "a",
			ModuleID:      "test-module1",
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(activations.References))
		require.Equal(t, "server1", activations.References[0].ServerID())
		require.Equal(t, "server1_address", activations.References[0].ServerState().Address())
		require.Equal(t, "ns1", activations.References[0].Namespace())
		require.Equal(t, "ns1", activations.References[0].ModuleID().Namespace)
		require.Equal(t, "test-module1", activations.References[0].ModuleID().ID)
		require.Equal(t, "ns1", activations.References[0].ActorID().Namespace)
		require.Equal(t, "a", activations.References[0].ActorID().ID)
	}

	// Reuse the same actor ID, but with a different module. The registry should consider
	// it a completely separate entity therefore it will go on a different server.
	activations, err = registry.EnsureActivation(ctx, EnsureActivationRequest{
		Namespace:     "ns1",
		ActorID:       "a",
		ModuleID:      "test-module2",
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(activations.References))
	require.Equal(t, "server2", activations.References[0].ServerID())
	require.Equal(t, "server2_address", activations.References[0].ServerState().Address())
	require.Equal(t, "ns1", activations.References[0].Namespace())
	require.Equal(t, "ns1", activations.References[0].ModuleID().Namespace)
	require.Equal(t, "test-module2", activations.References[0].ModuleID().ID)
	require.Equal(t, "ns1", activations.References[0].ActorID().Namespace)
	require.Equal(t, "a", activations.References[0].ActorID().ID)
	require.True(t, activations.VersionStamp > prevVS)

	// Next 10 activations should all go to server2 for balancing purposes.
	for i := 0; i < 10; i++ {
		actorID := fmt.Sprintf("0-%d", i)
		activations, err = registry.EnsureActivation(ctx, EnsureActivationRequest{
			Namespace:     "ns1",
			ActorID:       actorID,
			ModuleID:      "test-module1",
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(activations.References))
		require.Equal(t, "server2", activations.References[0].ServerID())

		_, err = registry.Heartbeat(ctx, "server2", HeartbeatState{
			NumActivatedActors: i + 1,
			Address:            "server2_address",
		})
		require.NoError(t, err)
	}

	// Subsequent activations should load balance.
	var lastServerID string
	for i := 0; i < 10; i++ {
		actorID := fmt.Sprintf("1-%d", i)
		activations, err = registry.EnsureActivation(ctx, EnsureActivationRequest{
			Namespace:     "ns1",
			ActorID:       actorID,
			ModuleID:      "test-module1",
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(activations.References))

		if lastServerID == "" {
		} else if lastServerID == "server1" {
			require.Equal(t, "server2", activations.References[0].ServerID())
		} else {
			require.Equal(t, "server1", activations.References[0].ServerID())
		}
		_, err = registry.Heartbeat(ctx, activations.References[0].ServerID(), HeartbeatState{
			NumActivatedActors: 10 + i + 1,
			Address:            fmt.Sprintf("%s_address", activations.References[0].ServerID()),
		})
		require.NoError(t, err)
		lastServerID = activations.References[0].ServerID()
	}

	// Wait for server1's heartbeat to expire.
	//
	// TODO: Sleeps in tests are bad, but I'm lazy to inject a clock right now and deal
	//       with all of that.
	time.Sleep(HeartbeatTTL + time.Second)

	// Heartbeat server2. After this, the Registry should only consider server2 to be alive.
	_, err = registry.Heartbeat(ctx, "server2", HeartbeatState{
		NumActivatedActors: 9999999,
		Address:            "server2_address",
	})
	require.NoError(t, err)

	// Even though server2's NumActivatedActors value is very high, all activations will go to
	// server2 because its the only one available.
	for i := 0; i < 10; i++ {
		actorID := fmt.Sprintf("2-%d", i)
		activations, err = registry.EnsureActivation(ctx, EnsureActivationRequest{
			Namespace:     "ns1",
			ActorID:       actorID,
			ModuleID:      "test-module1",
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(activations.References))
		require.Equal(t, "server2", activations.References[0].ServerID())
	}
}
