package main

import (
	"context"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/richardartoul/nola/virtual"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/registry/leaderregistry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"

	"github.com/stretchr/testify/require"
)

const (
	namespace = "memorybalancer-namespace"
	module    = "memory-hog-module"

	baseRegistryPort = 12000
	baseEnvPort      = 13000

	numActors = 10
)

var (
	nextServerPort = int64(0)
)

// TestMemoryBalancing is an integration test / example that tests/demonstrates how
// NOLA can be leveraged to load balance actors in a cluster automatically based on
// their memory usage. The test creates 3 different servers running on different ports
// connected via the LeaderRegistry implementation. After spawning some actors, the
// test makes one of the actors begin using large amounts of memory. The test then
// asserts that the cluster eventually rebalances so that the actor using large amounts
// of memory is eventually isolated alone on 1 server with all other actors evenly
// balanced between the two other servers.
func TestMemoryBalancing(t *testing.T) {
	t.Parallel()

	var (
		lp          = &leaderProvider{}
		portServer1 = int(atomic.AddInt64(&nextServerPort, 1))
	)
	lp.setLeader(registry.Address{
		IP:   net.ParseIP("127.0.0.1"),
		Port: baseRegistryPort + portServer1,
	})

	var (
		server1, _, cleaupFn1 = newServer(t, lp, portServer1)
		server2, _, cleaupFn2 = newServer(t, lp, int(atomic.AddInt64(&nextServerPort, 1)))
		server3, _, cleaupFn3 = newServer(t, lp, int(atomic.AddInt64(&nextServerPort, 1)))
		servers               = []virtual.Debug{server1, server2, server3}
	)
	defer cleaupFn1()
	defer cleaupFn2()
	defer cleaupFn3()

	// Sleep for a few seconds to let the server heartbeat a few times otherwise actor
	// invocations will fail due to MinSuccessiveHeartbeatsBeforeAllowActivations.
	time.Sleep(5 * time.Second)

	for i := 0; i < numActors; i++ {
		_, err := server1.InvokeActor(
			context.Background(), namespace, actorID(i), module, "keep-alive", nil, types.CreateIfNotExist{})
		require.NoError(t, err)

		// Perform manual heartbeats to make sure the actors will be evenly distributed based on latest updates.
		for _, server := range servers {
			err = server.Heartbeat()
			require.NoError(t, err)
		}
	}

	require.True(t, server1.NumActivatedActors() == numActors/3 || server1.NumActivatedActors() == numActors/3+1)
	require.True(t, server2.NumActivatedActors() == numActors/3 || server2.NumActivatedActors() == numActors/3+1)
	require.True(t, server3.NumActivatedActors() == numActors/3 || server3.NumActivatedActors() == numActors/3+1)

	// Now, make one of the processes use way more memory than the others.
	for i := 0; ; i++ {
		time.Sleep(1 * time.Millisecond)

		_, err := server1.InvokeActor(
			context.Background(), namespace, actorID(0), module, "inc-memory-usage", nil, types.CreateIfNotExist{})
		require.NoError(t, err)

		for j := 0; j < numActors; j++ {
			if i == 0 { // i not j intentionally.
				// Ensure every actor has non-zero memory usage because the memory balancing functionality only
				// kicks in if there is more than 1 actor with > 0 memory usage on a server. I.E if a server has
				// a single actor using way too much memory, but its the only actor on the server using any memory
				// then no rebalancing will be done because moving a single actor will just move the problem somewhere
				// else. However, if there are 2 actors using > 0 memory and the server is overloaded in terms of
				// memory usage, the one with the lowest memory usage will be migrated away.
				_, err := server1.InvokeActor(
					context.Background(), namespace, actorID(j), module, "inc-memory-usage", nil, types.CreateIfNotExist{})
				require.NoError(t, err)
			}

			_, err := server1.InvokeActor(
				context.Background(), namespace, actorID(j), module, "keep-alive", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
		}

		var (
			numActorsServer1 = server1.NumActivatedActors()
			numActorsServer2 = server2.NumActivatedActors()
			numActorsServer3 = server3.NumActivatedActors()
		)
		// env1 should get drained down to 1 actor as all the low memory usage actors are drained
		// away and only the high memory usage actor remains.
		if numActorsServer1 != 1 {
			continue
		}

		// Eventually env2/env3 should stabilize with roughly the same number of actors.
		delta := int(math.Abs(float64(numActorsServer2) - float64(numActorsServer3)))
		if delta > 1 {
			continue
		}

		// Finally, all actors should be activated.
		sum := numActorsServer1 + numActorsServer2 + numActorsServer3
		if sum != numActors {
			continue
		}

		// All balancing criteria have been met, we're done.
		break
	}
}

// TestSurviveLeaderFailure tests that the implementation can tolerate failures of
// the leader and continue serving requests for actors that are already activated
// and whose activation is cached in-memory even if new actors cannot be activated
// in the meantime.
func TestSurviveLeaderFailure(t *testing.T) {
	t.Parallel()

	var (
		lp          = &leaderProvider{}
		portServer1 = int(atomic.AddInt64(&nextServerPort, 1))
	)
	lp.setLeader(registry.Address{
		IP:   net.ParseIP("127.0.0.1"),
		Port: baseRegistryPort + portServer1,
	})

	var (
		// Make one of the servers run the registry only, but no virtual environment
		// so that when we "kill" the leader we don't lose any actors.
		// TODO: We should add this as a setting to the leaderregistry to make it so
		//       the leader never assigns itself any actors and if it has any once
		//       it becomes the leader, it will drain them.
		reg1                   = newRegistry(t, lp, portServer1)
		server2, _, cleanupFn2 = newServer(t, lp, int(atomic.AddInt64(&nextServerPort, 1)))
		server3, _, cleanupFn3 = newServer(t, lp, int(atomic.AddInt64(&nextServerPort, 1)))
		servers                = []virtual.Debug{server2, server3}
	)
	defer reg1.Close(context.Background())
	defer cleanupFn2()
	defer cleanupFn3()

	// Sleep for a few seconds to let the server heartbeat a few times otherwise actor
	// invocations will fail due to MinSuccessiveHeartbeatsBeforeAllowActivations.
	time.Sleep(5 * time.Second)

	for i := 0; i < numActors; i++ {
		_, err := server2.InvokeActor(
			context.Background(), namespace, actorID(i), module, "keep-alive", nil, types.CreateIfNotExist{})
		require.NoError(t, err)

		// Perform manual heartbeats to make sure the actors will be evenly distributed based on latest updates.
		for _, server := range servers {
			err = server.Heartbeat()
			require.NoError(t, err)
		}
	}

	fmt.Printf("server2.NumActivatedActors: %d\n", server2.NumActivatedActors())
	require.True(t, server2.NumActivatedActors() == numActors/2 || server2.NumActivatedActors() == numActors/2+1)
	fmt.Printf("server3.NumActivatedActors: %d\n", server3.NumActivatedActors())
	require.True(t, server3.NumActivatedActors() == numActors/2 || server3.NumActivatedActors() == numActors/2+1)

	require.NoError(t, reg1.Close(context.Background()))

	start := time.Now()
	for i := 0; ; i++ {
		time.Sleep(10 * time.Millisecond)

		_, err := server2.InvokeActor(
			context.Background(), namespace, actorID(0), module, "inc-memory-usage", nil, types.CreateIfNotExist{})
		require.NoError(t, err)

		for j := 0; j < numActors; j++ {
			if i == 0 { // i not j intentionally.
				// Ensure every actor has non-zero memory usage because the memory balancing functionality only
				// kicks in if there is more than 1 actor with > 0 memory usage on a server. I.E if a server has
				// a single actor using way too much memory, but its the only actor on the server using any memory
				// then no rebalancing will be done because moving a single actor will just move the problem somewhere
				// else. However, if there are 2 actors using > 0 memory and the server is overloaded in terms of
				// memory usage, the one with the lowest memory usage will be migrated away.
				_, err := server2.InvokeActor(
					context.Background(), namespace, actorID(j), module, "inc-memory-usage", nil, types.CreateIfNotExist{})
				require.NoError(t, err)
			}

			_, err := server2.InvokeActor(
				context.Background(), namespace, actorID(j), module, "keep-alive", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
		}

		if time.Since(start) > time.Minute {
			// It's impossible for this test to "prove" that we can keep running forever, but if we can keep running
			// for 1 minute we'll assume everything is implemented correctly to handle leader failures. This is a
			// bit risky because technically we could have done something dumb like cache activations for 62s, but
			// its good enough for now.
			break
		}
	}
}

// TestSurviveLeaderFailureKillActors is the same as TestSurviveLeaderFailure, except we kill
// a server that is both the leader *and* running actors and then assert that by the end of the
// test the actors are live and reachable again.
func TestSurviveLeaderFailureKillActors(t *testing.T) {
	t.Parallel()

	var (
		lp          = &leaderProvider{}
		portServer1 = int(atomic.AddInt64(&nextServerPort, 1))
	)
	lp.setLeader(registry.Address{
		IP:   net.ParseIP("127.0.0.1"),
		Port: baseRegistryPort + portServer1,
	})

	var (
		server1, reg1, cleanupFn1 = newServer(t, lp, portServer1)
		portServer2               = int(atomic.AddInt64(&nextServerPort, 1))
		server2, _, cleanupFn2    = newServer(t, lp, portServer2)
		server3, _, cleanupFn3    = newServer(t, lp, int(atomic.AddInt64(&nextServerPort, 1)))
		servers                   = []virtual.Debug{server1, server2, server3}
	)
	defer cleanupFn1()
	defer cleanupFn2()
	defer cleanupFn3()

	// Sleep for a few seconds to let the server heartbeat a few times otherwise actor
	// invocations will fail due to MinSuccessiveHeartbeatsBeforeAllowActivations.
	time.Sleep(5 * time.Second)

	for i := 0; i < numActors; i++ {
		_, err := server2.InvokeActor(
			context.Background(), namespace, actorID(i), module, "keep-alive", nil, types.CreateIfNotExist{})
		require.NoError(t, err)

		// Perform manual heartbeats to make sure the actors will be evenly distributed based on latest updates.
		for _, server := range servers {
			err = server.Heartbeat()
			require.NoError(t, err)
		}
	}

	require.True(t, server1.NumActivatedActors() == numActors/3 || server1.NumActivatedActors() == numActors/3+1)
	require.True(t, server2.NumActivatedActors() == numActors/3 || server2.NumActivatedActors() == numActors/3+1)
	require.True(t, server3.NumActivatedActors() == numActors/3 || server3.NumActivatedActors() == numActors/3+1)

	require.NoError(t, server1.Close(context.Background()))
	require.NoError(t, reg1.Close(context.Background()))
	// Perform the leader transition since server 1 is dead.
	lp.setLeader(registry.Address{
		IP:   net.ParseIP("127.0.0.1"),
		Port: baseRegistryPort + portServer2,
	})

	start := time.Now()
	for i := 0; ; i++ {
		time.Sleep(10 * time.Millisecond)

		for j := 0; j < numActors; j++ {
			// Errors here are ok because we killed one of the servers.
			_, err := server2.InvokeActor(
				context.Background(), namespace, actorID(j), module, "inc-memory-usage", nil, types.CreateIfNotExist{})
			if err != nil {
				continue
			}
			_, err = server2.InvokeActor(
				context.Background(), namespace, actorID(j), module, "keep-alive", nil, types.CreateIfNotExist{})
			if err != nil {
				continue
			}
		}

		if time.Since(start) > time.Minute {
			// However, once we reach the end of the test we need to ensure that the cluster has restored the
			// availability of every actor so we don't tolerate errors anymore.
			for j := 0; j < numActors; j++ {
				_, err := server2.InvokeActor(
					context.Background(), namespace, actorID(j), module, "keep-alive", nil, types.CreateIfNotExist{})
				require.NoError(t, err)
			}
			break
		}
	}
}

// TestHandleLeaderTransitionGracefully tests that the implementation handles leader failures gracefully by ensuring
// we don't relocate any actors in the general case of a leader transition.
func TestHandleLeaderTransitionGracefully(t *testing.T) {
	t.Parallel()

	var (
		lp          = &leaderProvider{}
		portServer1 = int(atomic.AddInt64(&nextServerPort, 1))
	)
	lp.setLeader(registry.Address{
		IP:   net.ParseIP("127.0.0.1"),
		Port: baseRegistryPort + portServer1,
	})

	var (
		// Make one of the servers run the registry only, but no virtual environment
		// so that when we "kill" the leader we don't lose any actors.
		// TODO: We should add this as a setting to the leaderregistry to make it so
		//       the leader never assigns itself any actors and if it has any once
		//       it becomes the leader, it will drain them.
		reg1                   = newRegistry(t, lp, portServer1)
		portServer2            = int(atomic.AddInt64(&nextServerPort, 1))
		server2, _, cleanupFn2 = newServer(t, lp, portServer2)
		server3, _, cleanupFn3 = newServer(t, lp, int(atomic.AddInt64(&nextServerPort, 1)))
		server4, _, cleanupFn4 = newServer(t, lp, int(atomic.AddInt64(&nextServerPort, 1)))
		servers                = []virtual.Debug{server2, server3, server4}
	)
	defer reg1.Close(context.Background())
	defer cleanupFn2()
	defer cleanupFn3()
	defer cleanupFn4()

	// Sleep for a few seconds to let the server heartbeat a few times otherwise actor
	// invocations will fail due to MinSuccessiveHeartbeatsBeforeAllowActivations.
	time.Sleep(5 * time.Second)

	for i := 0; i < numActors; i++ {
		_, err := server2.InvokeActor(
			context.Background(), namespace, actorID(i), module, "keep-alive", nil, types.CreateIfNotExist{})
		require.NoError(t, err)
	}

	require.True(t, server2.NumActivatedActors() == numActors/3 || server2.NumActivatedActors() == numActors/3+1)
	require.True(t, server3.NumActivatedActors() == numActors/3 || server3.NumActivatedActors() == numActors/3+1)
	require.True(t, server4.NumActivatedActors() == numActors/3 || server4.NumActivatedActors() == numActors/3+1)

	// Kill the old leader and make a different node the new leader.
	require.NoError(t, reg1.Close(context.Background()))
	lp.setLeader(registry.Address{
		IP:   net.ParseIP("127.0.0.1"),
		Port: baseRegistryPort + portServer2,
	})

	// Sleep for 6 seconds for the new leader heartbeat to be propagated.
	time.Sleep(6 * time.Second)

	start := time.Now()
	for i := 0; ; i++ {
		time.Sleep(1 * time.Millisecond)

		for j := 0; j < numActors; j++ {
			_, err := server2.InvokeActor(
				context.Background(), namespace, actorID(j), module, "keep-alive", nil, types.CreateIfNotExist{})
			require.NoError(t, err)

			// Perform manual heartbeats to make sure the actors will be evenly distributed based on latest updates.
			for _, server := range servers {
				err = server.Heartbeat()
				require.NoError(t, err)
			}
		}

		// A bit hacky, but if the number of actors on each server remains exactly the same for long enough then
		// we assume no actors were rerouted since we don't have a better way to assert that currently.
		require.True(
			t,
			server2.NumActivatedActors() == numActors/3 || server2.NumActivatedActors() == numActors/3+1,
			server2.NumActivatedActors())
		require.True(
			t,
			server3.NumActivatedActors() == numActors/3 || server3.NumActivatedActors() == numActors/3+1,
			server3.NumActivatedActors())
		require.True(
			t,
			server4.NumActivatedActors() == numActors/3 || server4.NumActivatedActors() == numActors/3+1,
			server4.NumActivatedActors())

		if time.Since(start) > time.Minute {
			// It's impossible for this test to "prove" that we can keep running forever, but if we can keep running
			// for 1 minute we'll assume everything is implemented correctly to handle leader failures. This is a
			// bit risky because technically we could have done something dumb like cache activations for 62s, but
			// its good enough for now.
			break
		}
	}
}

// TestSurviveReplicaFailureWithRandomStrategy tests the ability to survive replica failure using the random strategy.
// It validates that actor invocations can still succeed despite the loss of a server replica when using the random replica selection strategy.
// The test spawns actors with multiple replicas, simulates a server failure by closing one of the servers,
// and ensures that requests can still succeed using the retry policy.
//
// Test Steps:
// 1. Spawn actors with multiple replicas using the random replica selection strategy.
// 2. Ensure that the desired replication is achieved by repeatedly invoking the same actor creation operation.
// 3. Simulate a server failure by closing one of the servers.
// 4. Set the MaxNumRetries to 0 and verify that the actor invocation fails eventually.
// 5. Set the MaxNumRetries to 1 and verify that the actor invocation is retried once on other available replicas and succeeds.
// 6. Test that if we set the PerAttemptTimeout to an extremely low value, the actor invocation never succeeds.
func TestSurviveReplicaFailureWithRandomStrategy(t *testing.T) {
	t.Parallel()

	var (
		lp          = &leaderProvider{}
		portServer1 = int(atomic.AddInt64(&nextServerPort, 1))
	)
	lp.setLeader(registry.Address{
		IP:   net.ParseIP("127.0.0.1"),
		Port: baseRegistryPort + portServer1,
	})

	var (
		server1, _, cleanupFn1 = newServer(t, lp, portServer1)
		server2, _, cleanupFn2 = newServer(t, lp, int(atomic.AddInt64(&nextServerPort, 1)))
		server3, _, cleanupFn3 = newServer(t, lp, int(atomic.AddInt64(&nextServerPort, 1)))
	)
	defer cleanupFn1()
	defer cleanupFn2()
	defer cleanupFn3()

	// Sleep for a few seconds to let the servers heartbeat multiple times,
	// allowing actor invocations to succeed.
	time.Sleep(5 * time.Second)

	options := types.CreateIfNotExist{Options: types.ActorOptions{
		ExtraReplicas:       2, // The field is set to 2 to match the number of available servers in the test setup, which is 3.
		ReplicationStrategy: types.ReplicaSelectionStrategyRandom,
		RetryPolicy: types.RetryPolicy{
			PerAttemptTimeout: 0, // Set it to 0, to disable the timeouts.
		},
	}}

	// Use the require.Eventually function to repeatedly invoke the same actor creation operation
	// until the desired replication is achieved.
	require.Eventually(t, func() bool {
		_, err := server1.InvokeActor(
			context.Background(), namespace, actorID(0), module, "keep-alive", nil, options)
		require.NoError(t, err)

		// Check if the total number of activated actors in every server is 1,
		// so that across all servers is equal to 3, indicating that each server
		// has successfully activated one replica of the actor.
		return server1.NumActivatedActors() == 1 && server2.NumActivatedActors() == 1 && server3.NumActivatedActors() == 1
	}, 10*time.Second, time.Microsecond, "actor should be eventually be replicated in all replicas.")

	// Close one of the servers to trigger invocation failures and test the retry policy.
	server3.Close(context.Background())

	// Set the MaxNumRetries to 0, which means no retries will be attempted.
	// We expect the actor invocation to fail without any retry attempts.
	options.Options.RetryPolicy.MaxNumRetries = 0
	require.Eventually(t, func() bool {
		_, err := server1.InvokeActor(
			context.Background(), namespace, actorID(0), module, "keep-alive", nil, options)
		return err != nil
	}, time.Second, time.Microsecond, "actor should eventually fail to invoke, with no retries and one of the replicas down.")

	// Set the MaxNumRetries set to 1, which allows for one retry attempt.
	// We expect the actor invocation to be retried once on other available replicas and succeed without any failures.
	options.Options.RetryPolicy.MaxNumRetries = 1
	for i := 0; i < 10; i++ {
		_, err := server1.InvokeActor(
			context.Background(), namespace, actorID(0), module, "keep-alive", nil, options)
		require.NoError(t, err, "actor should never fail to invoke with enough retries and alive replicas.")
	}

	// Test that if we set the timeout to an extremely low value, the actor invocation never succeeds.
	options.Options.RetryPolicy.PerAttemptTimeout = time.Nanosecond
	for i := 0; i < 10; i++ {
		_, err := server1.InvokeActor(
			context.Background(), namespace, actorID(0), module, "keep-alive", nil, options)
		require.Error(t, err, "actor invocation should never succeed with extremely low timeout.")
	}
}

// TestSurviveReplicaFailureWithSortedStrategy tests the ability to survive replica failure with a sorted strategy (biased towards a single replica).
// It validates that the actor can handle replica failures and still maintain high availability.
// The test creates an actor with multiple replicas, ensures that the actor is replicated only on the server that is biased towards,
// simulates a server failure by closing the biased server, and verifies the behavior of different retry policies.
//
// Test Steps:
// 1. Create multiple server instances.
// 2. Sleep for a few seconds to let the servers heartbeat multiple times, allowing actor invocations to succeed.
// 3. Create an actor with extra replicas and a sorted replication strategy.
// 4. Ensure that the actor is replicated only on one of the servers that is biased towards.
// 5. Select a biased server and a non-biased server. The biased server is used to close it, and the non-biased server is used for performing invocations.
// 6. Close the server that the replication is biased towards.
// 7. Set the MaxNumRetries to 0 and verify that the actor invocation fails without retries.
// 8. Set the MaxNumRetries to 1 and verify that the actor invocation is retried once on other available replicas and succeeds.
// 9. Test that if we set the PerAttemptTimeout to an extremely low value, the actor invocation never succeeds.
func TestSurviveReplicaFailureWithSortedStrategy(t *testing.T) {
	t.Parallel()

	var (
		lp          = &leaderProvider{}
		portServer1 = int(atomic.AddInt64(&nextServerPort, 1))
	)
	lp.setLeader(registry.Address{
		IP:   net.ParseIP("127.0.0.1"),
		Port: baseRegistryPort + portServer1,
	})

	var (
		server1, _, cleanupFn1 = newServer(t, lp, portServer1)
		server2, _, cleanupFn2 = newServer(t, lp, int(atomic.AddInt64(&nextServerPort, 1)))
		server3, _, cleanupFn3 = newServer(t, lp, int(atomic.AddInt64(&nextServerPort, 1)))
	)
	defer cleanupFn1()
	defer cleanupFn2()
	defer cleanupFn3()

	// Sleep for a few seconds to let the servers heartbeat multiple times,
	// allowing actor invocations to succeed.
	time.Sleep(5 * time.Second)

	options := types.CreateIfNotExist{Options: types.ActorOptions{
		ExtraReplicas:       2, // The field is set to 2 to match the number of available servers in the test setup, which is 3.
		ReplicationStrategy: types.ReplicaSelectionStrategySorted,
		RetryPolicy: types.RetryPolicy{
			PerAttemptTimeout: 0, // Set it to 0, to disable the timeouts.
		},
	}}

	// Ensure that the actor is replicated on one of the servers.
	require.Eventually(t, func() bool {
		_, err := server1.InvokeActor(
			context.Background(), namespace, actorID(0), module, "keep-alive", nil, options)
		require.NoError(t, err)

		// Check if the total number of activated actors in the biased server is never greater than 1,
		// indicating that the actor has successfully activated one replica on a server.
		numActivatedActors := server1.NumActivatedActors() + server2.NumActivatedActors() + server3.NumActivatedActors()
		return numActivatedActors == 1
	}, 5*time.Second, time.Microsecond, "actor should be replicated on one server.")

	// Ensure that the actor is replicated only on the server that is biased towards.
	for i := 0; i < 10; i++ {
		_, err := server1.InvokeActor(
			context.Background(), namespace, actorID(0), module, "keep-alive", nil, options)
		require.NoError(t, err)

		// Check if the total number of activated actors in the biased server is never greater than 1,
		// indicating that the actor has successfully activated one replica on a server.
		numActivatedActors := server1.NumActivatedActors() + server2.NumActivatedActors() + server3.NumActivatedActors()
		require.LessOrEqual(t, numActivatedActors, 1, "actor should only be replicated on the biased server.")
	}

	// Select a biased server and a non-biased server.
	// The biased server is used to simulate a replica failure by closing it, while the non-biased server is used for performing invocations.
	// We choose the non-biased server for invocations because it is expected to remain available, allowing us to test the retry behavior and resilience.
	var biasedServer, nonBiasedServer virtual.Environment
	if server1.NumActivatedActors() > 0 {
		biasedServer = server1
		nonBiasedServer = server2
	} else if server2.NumActivatedActors() > 0 {
		biasedServer = server2
		nonBiasedServer = server3
	} else {
		biasedServer = server3
		nonBiasedServer = server1
	}

	// Close the server that the replication is biased towards.
	biasedServer.Close(context.Background())

	// Set it to 0, to disable the timeouts.
	options.Options.RetryPolicy.PerAttemptTimeout = 0

	// Set the MaxNumRetries to 0, which means no retries will be attempted.
	// We expect the actor invocation to fail without any retry attempts, because it will try to invoke on the biased actor.
	options.Options.RetryPolicy.MaxNumRetries = 0
	_, err := nonBiasedServer.InvokeActor(
		context.Background(), namespace, actorID(0), module, "keep-alive", nil, options)
	require.Error(t, err, "actor invocation should fail in the first attempt, with no retries and biased server down.")

	// Set the MaxNumRetries set to 1, which allows for one retry attempt.
	// We expect the actor invocation to be retried once on other available replicas and succeed without any failures.
	options.Options.RetryPolicy.MaxNumRetries = 1
	for i := 0; i < 10; i++ {
		_, err := nonBiasedServer.InvokeActor(
			context.Background(), namespace, actorID(0), module, "keep-alive", nil, options)
		require.NoError(t, err, "actor should never fail to invoke with enough retries and alive replicas.")
	}

	// Test that if we set the timeout to an extremely low value, the actor invocation never succeeds.
	options.Options.RetryPolicy.PerAttemptTimeout = time.Nanosecond
	for i := 0; i < 10; i++ {
		_, err := server1.InvokeActor(
			context.Background(), namespace, actorID(0), module, "keep-alive", nil, options)
		require.Error(t, err, "actor invocation should never succeed with extremely low timeout.")
	}
}

func newServer(
	t *testing.T,
	lp leaderregistry.LeaderProvider,
	idx int,
) (virtual.Environment, registry.Registry, func()) {
	reg := newRegistry(t, lp, idx)

	var (
		envServerID = fmt.Sprintf("env-server-%d", idx)
		envPort     = baseEnvPort + idx
	)
	env, err := virtual.NewEnvironment(
		context.Background(), envServerID, reg, registry.NewNoopModuleStore(), virtual.NewHTTPClient(),
		virtual.EnvironmentOptions{
			Discovery: virtual.DiscoveryOptions{
				DiscoveryType:               virtual.DiscoveryTypeLocalHost,
				Port:                        envPort,
				AllowFailedInitialHeartbeat: true,
			},
			// Need to set this otherwise the environment will detect the address is localhost and just
			// do everything in-memory which is not what we want since we're trying to simulate a fairly
			// real scenario.
			ForceRemoteProcedureCalls: true,
			// Speedup actor GC so the test finishes faster.
			GCActorsAfterDurationWithNoInvocations: 5 * time.Second,
			// Test assumes the activation cache does not last very long. If the default activation cache
			// duration was increased too much the tests might end up just testing the caching behavior
			// instead of ensuring everything works even when the cache TTL expires so we hard-code it
			// to 5s here just to be safe.
			ActivationCacheTTL: 5 * time.Second,
		})
	require.NoError(t, err)
	require.NoError(t, env.RegisterGoModule(types.NewNamespacedIDNoType(namespace, module), &testModule{}))

	server := virtual.NewServer(registry.NewNoopModuleStore(), env)
	go func() {
		if err := server.Start(envPort); err != nil {
			if strings.Contains(err.Error(), "closed") {
				return
			}
			panic(err)
		}
	}()

	return env, reg, func() {
		reg.Close(context.Background())
		env.Close(context.Background())
		server.Stop(context.Background())
	}
}

func newRegistry(
	t *testing.T,
	lp leaderregistry.LeaderProvider,
	idx int,
) registry.Registry {
	var (
		registryServerID = fmt.Sprintf("registry-server-%d", idx)
		registryPort     = baseRegistryPort + idx
	)
	reg, err := leaderregistry.NewLeaderRegistry(
		context.Background(), lp, registryServerID, virtual.EnvironmentOptions{
			Discovery: virtual.DiscoveryOptions{
				DiscoveryType: virtual.DiscoveryTypeLocalHost,
				Port:          registryPort,
			},
		})
	require.NoError(t, err)

	return reg
}

type leaderProvider struct {
	sync.Mutex

	leader registry.Address
}

func (l *leaderProvider) setLeader(addr registry.Address) {
	l.Lock()
	defer l.Unlock()

	l.leader = addr
}

func (l *leaderProvider) GetLeader() (registry.Address, error) {
	l.Lock()
	defer l.Unlock()

	return l.leader, nil
}

type testModule struct {
}

func (tm testModule) Instantiate(
	ctx context.Context,
	reference types.ActorReferenceVirtual,
	payload []byte,
	host virtual.HostCapabilities,
) (virtual.Actor, error) {
	return &testActor{}, nil
}

func (tm testModule) Close(ctx context.Context) error {
	return nil
}

type testActor struct {
	count int
}

func (ta *testActor) MemoryUsageBytes() int {
	return ta.count * 1024 * 1024
}

func (ta *testActor) Invoke(
	ctx context.Context,
	operation string,
	payload []byte,
) ([]byte, error) {
	switch operation {
	case wapcutils.StartupOperationName:
		return nil, nil
	case wapcutils.ShutdownOperationName:
		return nil, nil
	case "keep-alive":
		return nil, nil
	case "inc-memory-usage":
		ta.count++
		return nil, nil
	default:
		return nil, fmt.Errorf("testActor: unhandled operation: %s", operation)
	}
}

func (ta *testActor) Close(
	ctx context.Context,
) error {
	return nil
}

func actorID(idx int) string {
	return fmt.Sprintf("actor-%d", idx)
}
