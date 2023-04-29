package registry

import (
	"context"
	"fmt"
	"strings"
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

	t.Run("kv simple", func(t *testing.T) {
		testKVSimple(t, registryCtor())
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
	_, err := registry.EnsureActivation(ctx, "ns1", "a", "test-module1")
	require.Error(t, err)
	require.False(t, IsActorDoesNotExistErr(err))

	heartbeatResult, err := registry.Heartbeat(ctx, "server1", HeartbeatState{
		NumActivatedActors: 10,
		Address:            "server1_address",
	})
	require.NoError(t, err)
	require.True(t, heartbeatResult.VersionStamp > 0)
	require.Equal(t, HeartbeatTTL.Microseconds(), heartbeatResult.HeartbeatTTL)

	// Should succeed now that we have a server to activate on.
	activations, err := registry.EnsureActivation(ctx, "ns1", "a", "test-module1")
	require.NoError(t, err)
	require.Equal(t, 1, len(activations))
	require.Equal(t, "server1", activations[0].ServerID())
	require.Equal(t, "server1_address", activations[0].Address())
	require.Equal(t, "ns1", activations[0].Namespace())
	require.Equal(t, "ns1", activations[0].ModuleID().Namespace)
	require.Equal(t, "test-module1", activations[0].ModuleID().ID)
	require.Equal(t, "ns1", activations[0].ActorID().Namespace)
	require.Equal(t, "a", activations[0].ActorID().ID)
	require.Equal(t, uint64(1), activations[0].Generation())

	// Ensure we get back all the same information but with the generation
	// bumped now.
	require.NoError(t, registry.IncGeneration(ctx, "ns1", "a", "test-module1"))
	activations, err = registry.EnsureActivation(ctx, "ns1", "a", "test-module1")
	require.NoError(t, err)
	require.Equal(t, 1, len(activations))
	require.Equal(t, "server1", activations[0].ServerID())
	require.Equal(t, "server1_address", activations[0].Address())
	require.Equal(t, "ns1", activations[0].Namespace())
	require.Equal(t, "ns1", activations[0].ModuleID().Namespace)
	require.Equal(t, "test-module1", activations[0].ModuleID().ID)
	require.Equal(t, "ns1", activations[0].ActorID().Namespace)
	require.Equal(t, "a", activations[0].ActorID().ID)
	require.Equal(t, uint64(2), activations[0].Generation())

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
		activations, err := registry.EnsureActivation(ctx, "ns1", "a", "test-module1")
		require.NoError(t, err)
		require.Equal(t, 1, len(activations))
		require.Equal(t, "server1", activations[0].ServerID())
		require.Equal(t, "server1_address", activations[0].Address())
		require.Equal(t, "ns1", activations[0].Namespace())
		require.Equal(t, "ns1", activations[0].ModuleID().Namespace)
		require.Equal(t, "test-module1", activations[0].ModuleID().ID)
		require.Equal(t, "ns1", activations[0].ActorID().Namespace)
		require.Equal(t, "a", activations[0].ActorID().ID)
	}

	// Reuse the same actor ID, but with a different module. The registry should consider
	// it a completely separate entity therefore it will go on a different server.
	activations, err = registry.EnsureActivation(ctx, "ns1", "a", "test-module2")
	require.NoError(t, err)
	require.Equal(t, 1, len(activations))
	require.Equal(t, "server2", activations[0].ServerID())
	require.Equal(t, "server2_address", activations[0].Address())
	require.Equal(t, "ns1", activations[0].Namespace())
	require.Equal(t, "ns1", activations[0].ModuleID().Namespace)
	require.Equal(t, "test-module2", activations[0].ModuleID().ID)
	require.Equal(t, "ns1", activations[0].ActorID().Namespace)
	require.Equal(t, "a", activations[0].ActorID().ID)

	// Next 10 activations should all go to server2 for balancing purposes.
	for i := 0; i < 10; i++ {
		actorID := fmt.Sprintf("0-%d", i)
		activations, err = registry.EnsureActivation(ctx, "ns1", actorID, "test-module1")
		require.NoError(t, err)
		require.Equal(t, 1, len(activations))
		require.Equal(t, "server2", activations[0].ServerID())

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
		activations, err = registry.EnsureActivation(ctx, "ns1", actorID, "test-module1")
		require.NoError(t, err)
		require.Equal(t, 1, len(activations))

		if lastServerID == "" {
		} else if lastServerID == "server1" {
			require.Equal(t, "server2", activations[0].ServerID())
		} else {
			require.Equal(t, "server1", activations[0].ServerID())
		}
		_, err = registry.Heartbeat(ctx, activations[0].ServerID(), HeartbeatState{
			NumActivatedActors: 10 + i + 1,
			Address:            fmt.Sprintf("%s_address", activations[0].ServerID()),
		})
		require.NoError(t, err)
		lastServerID = activations[0].ServerID()
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
		activations, err = registry.EnsureActivation(ctx, "ns1", actorID, "test-module1")
		require.NoError(t, err)
		require.Equal(t, 1, len(activations))
		require.Equal(t, "server2", activations[0].ServerID())
	}
}

func testKVSimple(t *testing.T, registry Registry) {
	ctx := context.Background()
	defer registry.Close(ctx)

	for nsIdx, ns := range []string{"ns1", "ns2"} {
		_, err := registry.BeginTransaction(ctx, ns, "a", "test-module1", "server1", 0)
		if err != nil && strings.Contains(err.Error(), "not implemented") {
			t.Skip("skipping KV test for registry that does not implement KV")
		}

		// Cant start transaction for actor that doesn't exist (no call to EnsureActivation yet).
		require.Error(t, err)

		for actorIdx, actor := range []string{"1", "2", "3", "4", "5"} {
			func() {
				tr, err := registry.BeginTransaction(ctx, ns, "a", "test-module1", "server1", 0)
				// Cant start transaction for actor that doesn't exist (no call to EnsureActivation yet).
				require.Error(t, err)

				if nsIdx == 0 && actorIdx == 0 {
					// Server heartbeats span namespaces so this will only be true the
					// first time.

					// Cant ensure activation when no available servers.
					_, err = registry.EnsureActivation(ctx, ns, actor, "test-module1")
					require.Error(t, err)

					// Heartbeat server so we can activate.
					_, err = registry.Heartbeat(ctx, "server1", HeartbeatState{
						NumActivatedActors: 0,
						Address:            "server1_address",
					})
					require.NoError(t, err)
				}

				// Same actor ID, but different modules, should end up with separate KV storage.
				_, err = registry.EnsureActivation(ctx, ns, actor, "test-module1")
				require.NoError(t, err)
				_, err = registry.EnsureActivation(ctx, ns, actor, "test-module2")
				require.NoError(t, err)

				for _, module := range []string{"test-module1", "test-module2"} {
					func() {
						var (
							rightServer = "server1"
							wrongServer = "server2"
						)
						if module == "module2" {
							rightServer, wrongServer = wrongServer, rightServer
						}
						tr, err = registry.BeginTransaction(ctx, ns, actor, module, wrongServer, 0)
						// Cant start transaction for actor from wrong server.
						require.Error(t, err)

						tr, err = registry.BeginTransaction(ctx, ns, actor, module, rightServer, 0)
						// Cant start transaction for actor with stale server version.
						require.Error(t, err)

						// Finally now that we've created the actor, created a live server, ensured the
						// actor is activated on the live server, and initiate the transaction from the
						// server the actor should be activated on, we can begin a transaction.
						tr, err = registry.BeginTransaction(ctx, ns, actor, module, rightServer, 1)
						require.NoError(t, err)
						defer func() {
							require.NoError(t, tr.Commit(ctx))
						}()

						for i := 0; i < 10; i++ {
							var (
								key   = []byte(fmt.Sprintf("key-%d", i))
								value = []byte(fmt.Sprintf("%s::%s::%d", ns, actor, i))
							)
							// PUT/GET should work now.
							_, ok, err := tr.Get(ctx, key)
							require.NoError(t, err)
							// key1 should not exist yet.
							require.False(t, ok)

							// Store key1 now. Subsequent GET should work.
							err = tr.Put(ctx, key, value)
							require.NoError(t, err)

							val, ok, err := tr.Get(ctx, key)
							require.NoError(t, err)
							require.True(t, ok)
							require.Equal(t, value, val)
						}

						// Make sure we can re-read all the keys.
						for i := 0; i < 10; i++ {
							var (
								key   = []byte(fmt.Sprintf("key-%d", i))
								value = []byte(fmt.Sprintf("%s::%s::%d", ns, actor, i))
							)
							val, ok, err := tr.Get(ctx, key)
							require.NoError(t, err)
							require.True(t, ok)
							require.Equal(t, value, val)
						}
					}()
				}
			}()
		}
	}
}
