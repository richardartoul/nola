package registry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/richardartoul/nola/virtual/types"
	"github.com/stretchr/testify/require"
)

// TODO: Add some concurrency tests.

// This is called from the specific registry implementation subpackages like
// fdbregistry, localregistry, dnsregistry, etc.
func TestAllCommon(t *testing.T, registryCtor func() Registry) {
	t.Run("service discovery and ensure activation", func(t *testing.T) {
		testRegistryServiceDiscoveryAndEnsureActivation(t, registryCtor())
	})

	t.Run("test registry replication", func(t *testing.T) {
		testRegistryReplication(t, registryCtor())
	})

	t.Run("test ensure activations persistence", func(t *testing.T) {
		testEnsureActivationPersistence(t, registryCtor())
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
		Namespace: "ns1",
		ActorID:   "a",
		ModuleID:  "test-module1",
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(activations.References))
	require.Equal(t, "server1", activations.References[0].Physical.ServerID)
	require.Equal(t, "server1_address", activations.References[0].Physical.ServerState.Address)
	require.Equal(t, "ns1", activations.References[0].Virtual.Namespace)
	require.Equal(t, "ns1", activations.References[0].Virtual.Namespace)
	require.Equal(t, "test-module1", activations.References[0].Virtual.ModuleID)
	require.Equal(t, "a", activations.References[0].Virtual.ActorID)
	require.Equal(t, uint64(1), activations.References[0].Virtual.Generation)
	require.True(t, activations.VersionStamp > 0)
	prevVS := activations.VersionStamp

	activations, err = registry.EnsureActivation(ctx, EnsureActivationRequest{
		Namespace: "ns1",
		ActorID:   "a",
		ModuleID:  "test-module1",
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(activations.References))
	require.Equal(t, "server1", activations.References[0].Physical.ServerID)
	require.Equal(t, "server1_address", activations.References[0].Physical.ServerState.Address)
	require.Equal(t, "ns1", activations.References[0].Virtual.Namespace)
	require.Equal(t, "test-module1", activations.References[0].Virtual.ModuleID)
	require.Equal(t, "a", activations.References[0].Virtual.ActorID)
	require.Equal(t, uint64(1), activations.References[0].Virtual.Generation)
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
			Namespace: "ns1",
			ActorID:   "a",
			ModuleID:  "test-module1",
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(activations.References))
		require.Equal(t, "server1", activations.References[0].Physical.ServerID)
		require.Equal(t, "server1_address", activations.References[0].Physical.ServerState.Address)
		require.Equal(t, "ns1", activations.References[0].Virtual.Namespace)
		require.Equal(t, "test-module1", activations.References[0].Virtual.ModuleID)
		require.Equal(t, "a", activations.References[0].Virtual.ActorID)
	}

	// Reuse the same actor ID, but with a different module. The registry should consider
	// it a completely separate entity therefore it will go on a different server.
	activations, err = registry.EnsureActivation(ctx, EnsureActivationRequest{
		Namespace: "ns1",
		ActorID:   "a",
		ModuleID:  "test-module2",
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(activations.References))
	require.Equal(t, "server2", activations.References[0].Physical.ServerID)
	require.Equal(t, "server2_address", activations.References[0].Physical.ServerState.Address)
	require.Equal(t, "ns1", activations.References[0].Virtual.Namespace)
	require.Equal(t, "test-module2", activations.References[0].Virtual.ModuleID)
	require.Equal(t, "a", activations.References[0].Virtual.ActorID)
	require.True(t, activations.VersionStamp > prevVS)

	// Next 10 activations should all go to server2 for balancing purposes.
	for i := 0; i < 10; i++ {
		actorID := fmt.Sprintf("0-%d", i)
		activations, err = registry.EnsureActivation(ctx, EnsureActivationRequest{
			Namespace: "ns1",
			ActorID:   actorID,
			ModuleID:  "test-module1",
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(activations.References))
		require.Equal(t, "server2", activations.References[0].Physical.ServerID)

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
			Namespace: "ns1",
			ActorID:   actorID,
			ModuleID:  "test-module1",
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(activations.References))

		if lastServerID == "" {
		} else if lastServerID == "server1" {
			require.Equal(t, "server2", activations.References[0].Physical.ServerID)
		} else {
			require.Equal(t, "server1", activations.References[0].Physical.ServerID)
		}
		_, err = registry.Heartbeat(ctx, activations.References[0].Physical.ServerID, HeartbeatState{
			NumActivatedActors: 10 + i + 1,
			Address:            fmt.Sprintf("%s_address", activations.References[0].Physical.ServerID),
		})
		require.NoError(t, err)
		lastServerID = activations.References[0].Physical.ServerID
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
			Namespace: "ns1",
			ActorID:   actorID,
			ModuleID:  "test-module1",
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(activations.References))
		require.Equal(t, "server2", activations.References[0].Physical.ServerID)
	}
}

// testRegistryReplication is a test function that verifies the replication behavior of a registry implementation.
// The steps performed by this function are as follows:
//
//  1. Ensure Activation - Single Replica:
//     The function calls the `EnsureActivation` method again, this time without requesting any additional replicas.
//     This step verifies the successful activation of an actor with a single replica. The number of returned
//     activation references is validated to ensure that only one reference is returned.
//
//  2. Ensure Activation - Extra Replica:
//     The function calls the `EnsureActivation` method of the registry to ensure the activation of an actor with the
//     given namespace, actor ID, and module ID. In this step, an additional replica is requested for the actor.
//     The number of returned activation references is validated to ensure that two references are returned.
//
//  3. Ensure Activation - Extra Replicas:
//     The function calls the `EnsureActivation` method once more, but this time requests two additional replicas
//     for the actor. It's important to note that even though the function requests three replicas in total, the
//     replication behavior is limited by the number of available servers. In this case, since there are only two
//     servers ("server1" and "server2"), the maximum number of replicas that can be created is also limited to two.
//     The purpose of this step is to test the successful activation of an actor with the maximum number of replicas
//     that the available servers can accommodate. The number of returned activation references is validated to ensure
//     that two references are returned, indicating that the replication behavior respects the available server
//     resources and doesn't exceed the limit.
func testRegistryReplication(t *testing.T, registry Registry) {
	ctx := context.Background()
	defer registry.Close(ctx)

	for i := 0; i < 5; i++ {
		// Heartbeat 5 times because some registry implementations (like the
		// LeaderRegistry) require multiple successful heartbeats from at least
		// 1 server before any actors can be placed.
		heartbeatResult, err := registry.Heartbeat(ctx, "server1", HeartbeatState{
			NumActivatedActors: 10,
			Address:            "server1_address",
		})
		require.NoError(t, err)
		require.True(t, heartbeatResult.VersionStamp > 0)
		require.Equal(t, HeartbeatTTL.Microseconds(), heartbeatResult.HeartbeatTTL)

		heartbeatResult, err = registry.Heartbeat(ctx, "server2", HeartbeatState{
			NumActivatedActors: 10,
			Address:            "server2_address",
		})
		require.NoError(t, err)
		require.True(t, heartbeatResult.VersionStamp > 0)
		require.Equal(t, HeartbeatTTL.Microseconds(), heartbeatResult.HeartbeatTTL)
	}

	activations, err := registry.EnsureActivation(ctx, EnsureActivationRequest{
		Namespace: "ns1",
		ActorID:   "a",
		ModuleID:  "test-module1",
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(activations.References))

	activations, err = registry.EnsureActivation(ctx, EnsureActivationRequest{
		Namespace:     "ns1",
		ActorID:       "b",
		ModuleID:      "test-module1",
		ExtraReplicas: 1,
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(activations.References))

	activations, err = registry.EnsureActivation(ctx, EnsureActivationRequest{
		Namespace:     "ns1",
		ActorID:       "c",
		ModuleID:      "test-module1",
		ExtraReplicas: 2,
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(activations.References))
}

// The purpose of this test function is to verify the persistence of actor activations across consecutive calls to the EnsureActivation function.
// The logic of the test involves calling the EnsureActivation function every millisecond for a second and expecting to consistently receive
// the same actor reference (server) in return.
//
// The test is designed to check whether activations are persisted correctly, meaning that the same actor reference should be returned unless
// the server is blacklisted or goes down. It assumes that if activations are persisted, the EnsureActivation function will consistently return
// the same reference, unless exceptional circumstances such as server blacklisting or failure occur, which are not expected during the test.
//
// The test executes a series of heartbeats to simulate active servers and establish a stable environment. Then, it calls the EnsureActivation
// function and checks that the received actor reference remains the same throughout the test duration. If persistence is functioning as
// expected, the test should never encounter a different reference during the test. However, if persistence is not working correctly, there
// is a probability that at least once during the test, a different reference may be returned when ensuring activations for an actor.
//
// This test is valuable in ensuring the reliability and consistency of the activation persistence feature. By continuously monitoring the
// returned actor references, the test helps identify any issues related to persistence and ensures that activations remain stable and
// unchanged across multiple calls to the EnsureActivation function.
func testEnsureActivationPersistence(t *testing.T, registry Registry) {
	ctx := context.Background()
	defer registry.Close(ctx)

	for i := 0; i < 5; i++ {
		// Heartbeat 5 times because some registry implementations (like the
		// LeaderRegistry) require multiple successful heartbeats from at least
		// 1 server before any actors can be placed.
		heartbeatResult, err := registry.Heartbeat(ctx, "server1", HeartbeatState{
			NumActivatedActors: 10,
			Address:            "server1_address",
		})
		require.NoError(t, err)
		require.True(t, heartbeatResult.VersionStamp > 0)
		require.Equal(t, HeartbeatTTL.Microseconds(), heartbeatResult.HeartbeatTTL)

		heartbeatResult, err = registry.Heartbeat(ctx, "server2", HeartbeatState{
			NumActivatedActors: 10,
			Address:            "server2_address",
		})
		require.NoError(t, err)
		require.True(t, heartbeatResult.VersionStamp > 0)
		require.Equal(t, HeartbeatTTL.Microseconds(), heartbeatResult.HeartbeatTTL)
	}

	var ref types.ActorReference
	require.Never(t, func() bool {
		activations, err := registry.EnsureActivation(ctx, EnsureActivationRequest{
			Namespace: "ns1",
			ActorID:   "a",
			ModuleID:  "test-module1",
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(activations.References))
		differentActivation := !(ref == types.ActorReference{} || ref == activations.References[0])
		ref = activations.References[0]
		return differentActivation
	}, time.Second, time.Millisecond, "actor has been activated in more than one server")
}
