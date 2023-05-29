package virtual

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/registry/localregistry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"

	"github.com/stretchr/testify/require"
)

// We have some more integration tests in other packages, for example the package
// examples/leaderregistry has a few really good tests that are worth looking at
// as well.

var (
	streamInterfaceWasCalledMutex sync.Mutex
	streamInterfaceWasCalled      = false
	customHostFns                 = map[string]func([]byte) ([]byte, error){
		"testCustomFn": func([]byte) ([]byte, error) {
			return []byte("ok"), nil
		},
	}

	testDiscovery = DiscoveryOptions{
		DiscoveryType: DiscoveryTypeLocalHost,
	}
	// Override with a low default value to prevent tests from being slow.
	testGCActorsAfterDurationWithNoInvocations = 2 * time.Second

	utilWasmBytes   []byte
	defaultOptsWASM = EnvironmentOptions{
		Discovery:                              testDiscovery,
		CustomHostFns:                          customHostFns,
		GCActorsAfterDurationWithNoInvocations: testGCActorsAfterDurationWithNoInvocations,
	}
	defaultOptsGoByte = EnvironmentOptions{
		Discovery:                              testDiscovery,
		CustomHostFns:                          customHostFns,
		GCActorsAfterDurationWithNoInvocations: testGCActorsAfterDurationWithNoInvocations,
	}
	defaultOptsGoStream = EnvironmentOptions{
		Discovery:                              testDiscovery,
		CustomHostFns:                          customHostFns,
		GCActorsAfterDurationWithNoInvocations: testGCActorsAfterDurationWithNoInvocations,
	}
	defaultOptsGoDNS = EnvironmentOptions{
		CustomHostFns:                          customHostFns,
		GCActorsAfterDurationWithNoInvocations: testGCActorsAfterDurationWithNoInvocations,
	}
)

func init() {
	fBytes, err := ioutil.ReadFile("../testdata/tinygo/util/main.wasm")
	if err != nil {
		panic(err)
	}
	utilWasmBytes = fBytes
}

func TestMain(m *testing.M) {
	// Make sure this map is cleared between tests. We could just clear it
	// here, but its easier to  understand/debug if we just assert and fix
	// tests that dont close properly instead of trying to be clever and wipe
	// it automatically.
	localEnvironmentsRouterLock.Lock()
	if len(localEnvironmentsRouter) != 0 {
		localEnvironmentsRouterLock.Unlock()
		panic("test did not clear localEnvironmentsRoute")
	}
	localEnvironmentsRouterLock.Unlock()

	// Override constants to make the tests faster.
	oldDefaultActivationsCacheTTL := defaultActivationsCacheTTL
	defaultActivationsCacheTTL = 100 * time.Millisecond
	defer func() {
		defaultActivationsCacheTTL = oldDefaultActivationsCacheTTL
	}()

	code := m.Run()
	os.Exit(code)
}

// TODO: Need a good concurrency test that spans a bunch of goroutine and
//       spams registry operations + invocations.

// TestSimpleActor is a basic sanity test that verifies the most basic flow for actors.
func TestSimpleActor(t *testing.T) {
	testFn := func(t *testing.T, reg registry.Registry, env Environment) {
		ctx := context.Background()
		for _, ns := range []string{"ns-1", "ns-2"} {
			for i := 0; i < 100; i++ {
				result, err := env.InvokeActor(
					ctx, ns, "a", "test-module", "inc",
					nil, types.CreateIfNotExist{})
				require.NoError(t, err)
				require.Equal(t, int64(i+1), getCount(t, result))

				if i == 0 {
					result, err = env.InvokeActor(
						ctx, ns, "a", "test-module", "getStartupWasCalled",
						nil, types.CreateIfNotExist{})
					require.NoError(t, err)
					require.Equal(t, []byte("true"), result)
				}
			}
		}
	}

	runWithDifferentConfigs(t, testFn, nil, false, false, testGCActorsAfterDurationWithNoInvocations)
}

// TestCreateIfNotExist tests that the CreateIfNotExist argument can be used to invoke an actor and
// create it automatically if it does not already exist.
func TestCreateIfNotExist(t *testing.T) {
	testFn := func(t *testing.T, reg registry.Registry, env Environment) {
		ctx := context.Background()
		for _, ns := range []string{"ns-1", "ns-2"} {
			for i := 0; i < 100; i++ {
				result, err := env.InvokeActor(
					ctx, ns, "a", "test-module",
					"inc", nil, types.CreateIfNotExist{})
				require.NoError(t, err)
				require.Equal(t, int64(i+1), getCount(t, result))

				if i == 0 {
					result, err = env.InvokeActor(
						ctx, ns, "a", "test-module",
						"getStartupWasCalled", nil, types.CreateIfNotExist{})
					require.NoError(t, err)
					require.Equal(t, []byte("true"), result)
				}
			}
		}
	}

	runWithDifferentConfigs(t, testFn, nil, false, false, testGCActorsAfterDurationWithNoInvocations)
}

// TestCreateIfNotExistWithInstantiatePayload is the same as TestCreateIfNotExist
// except it also asserts the instantiation payload is propagated properly also.
func TestCreateIfNotExistWithInstantiatePayload(t *testing.T) {
	testFn := func(t *testing.T, reg registry.Registry, env Environment) {
		ctx := context.Background()
		for _, ns := range []string{"ns-1", "ns-2"} {
			for i := 0; i < 100; i++ {
				result, err := env.InvokeActor(
					ctx, ns, "a", "test-module",
					"inc", nil, types.CreateIfNotExist{InstantiatePayload: []byte("abc")})
				require.NoError(t, err)
				require.Equal(t, int64(i+1), getCount(t, result))

				if i == 0 {
					result, err = env.InvokeActor(
						ctx, ns, "a", "test-module",
						"getStartupWasCalled", nil, types.CreateIfNotExist{})
					require.NoError(t, err)
					require.Equal(t, []byte("true"), result)

					result, err = env.InvokeActor(
						ctx, ns, "a", "test-module",
						"getInstantiatePayload", nil, types.CreateIfNotExist{})
					require.NoError(t, err)
					require.Equal(t, []byte("abc"), result)
				}
			}
		}
	}

	runWithDifferentConfigs(t, testFn, nil, false, false, testGCActorsAfterDurationWithNoInvocations)
}

// TestSimpleWorker is a basic sanity test that verifies the most basic flow for workers.
func TestSimpleWorker(t *testing.T) {
	testFn := func(t *testing.T, reg registry.Registry, env Environment) {
		ctx := context.Background()
		for _, ns := range []string{"ns-1", "ns-2"} {
			// Can invoke immediately once module exists, no need to "create" a worker or anything
			// like we do for actors.
			_, err := env.InvokeWorker(
				ctx, ns, "test-module",
				"inc", nil, types.CreateIfNotExist{})
			require.NoError(t, err)

			// Workers can still "accumulate" in-memory state like actors do, but the state may vary
			// depending on which server/environment the call is executed on (unlike actors where the
			// request is always routed to the single active "global" instance).
			for i := 0; i < 100; i++ {
				result, err := env.InvokeWorker(
					ctx, ns, "test-module",
					"inc", nil, types.CreateIfNotExist{})
				require.NoError(t, err)
				require.Equal(t, int64(i+2), getCount(t, result))

				if i == 0 {
					result, err = env.InvokeWorker(
						ctx, ns, "test-module",
						"getStartupWasCalled", nil, types.CreateIfNotExist{})
					require.NoError(t, err)
					require.Equal(t, []byte("true"), result)
				}
			}
		}
	}

	runWithDifferentConfigs(t, testFn, nil, false, false, testGCActorsAfterDurationWithNoInvocations)
}

// TestInvokeActorHostFunction tests whether the invoke actor host function can be used
// by the WASM module to invoke operations on other actors on demand. In other words, this
// test ensures that actors can communicate with other actors.
func TestInvokeActorHostFunction(t *testing.T) {
	testFn := func(t *testing.T, reg registry.Registry, env Environment) {
		ctx := context.Background()
		for _, ns := range []string{"ns-1", "ns-2"} {
			// Create an actor, then immediately fork it so we have two actors.

			// Ensure actor a can communicate with actor b.
			invokeReq := types.InvokeActorRequest{
				ActorID:   "b",
				ModuleID:  "test-module",
				Operation: "inc",
				Payload:   nil,
			}
			marshaled, err := json.Marshal(invokeReq)
			require.NoError(t, err)
			_, err = env.
				InvokeActor(ctx, ns, "a", "test-module", "invokeActor", marshaled, types.CreateIfNotExist{})
			require.NoError(t, err)

			// Ensure actor b can communicate with actor a.
			invokeReq = types.InvokeActorRequest{
				ActorID:   "a",
				ModuleID:  "test-module",
				Operation: "inc",
				Payload:   nil,
			}
			marshaled, err = json.Marshal(invokeReq)
			require.NoError(t, err)
			_, err = env.
				InvokeActor(ctx, ns, "b", "test-module", "invokeActor", marshaled, types.CreateIfNotExist{})
			require.NoError(t, err)

			// Ensure both actor's state was actually updated and they can request
			// each other's state.
			invokeReq = types.InvokeActorRequest{
				ActorID:   "b",
				ModuleID:  "test-module",
				Operation: "getCount",
				Payload:   nil,
			}
			marshaled, err = json.Marshal(invokeReq)
			require.NoError(t, err)
			result, err := env.
				InvokeActor(ctx, ns, "a", "test-module", "invokeActor", marshaled, types.CreateIfNotExist{})
			require.NoError(t, err)
			require.Equal(t, int64(1), getCount(t, result))

			invokeReq = types.InvokeActorRequest{
				ActorID:   "a",
				ModuleID:  "test-module",
				Operation: "getCount",
				Payload:   nil,
			}
			marshaled, err = json.Marshal(invokeReq)
			require.NoError(t, err)
			result, err = env.
				InvokeActor(ctx, ns, "b", "test-module", "invokeActor", marshaled, types.CreateIfNotExist{})
			require.NoError(t, err)
			require.Equal(t, int64(1), getCount(t, result))
		}
	}

	runWithDifferentConfigs(t, testFn, nil, false, false, testGCActorsAfterDurationWithNoInvocations)
}

// TestScheduleSelfTimersAndGC tests whether actors can schedule invocations for themselves to run
// sometime in the future as a way to implement timers. It also tests the functionality of GCing
// actors after they receive no invocations for a period of time, as well as the interaction between
// timers and GC to ensure that timers that fire after an actor has been GC'd do not reinstantiate the actor.
func TestScheduleSelfTimersAndGC(t *testing.T) {
	gcDuration := 100 * time.Millisecond
	testFn := func(t *testing.T, reg registry.Registry, env Environment) {
		ctx := context.Background()
		for _, ns := range []string{"ns-1", "ns-2"} {
			selfTimerReq := wapcutils.ScheduleSelfTimer{
				Operation: "inc",
				Payload:   nil,
				// Make the timer fire before the actor is GC'd.
				AfterMillis: int(gcDuration.Milliseconds()) / 2,
			}
			marshaledSelfTimerReq, err := json.Marshal(selfTimerReq)
			require.NoError(t, err)

			// Schedule self increment in future.
			_, err = env.
				InvokeActor(ctx, ns, "a", "test-module", "scheduleSelfTimer", marshaledSelfTimerReq, types.CreateIfNotExist{})
			require.NoError(t, err)

			// Make sure a is 0 immediately after scheduling.
			result, err := env.
				InvokeActor(ctx, ns, "a", "test-module", "getCount", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
			require.Equal(t, int64(0), getCount(t, result))

			// Wait for timer to fire.
			for {
				result, err := env.
					InvokeActor(ctx, ns, "a", "test-module", "getCount", nil, types.CreateIfNotExist{})
				require.NoError(t, err)
				if getCount(t, result) != int64(1) {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				break
			}

			numActors := env.NumActivatedActors()
			require.Equal(t, 1, numActors)
			// Wait for actor to be GC'd.
			for {
				numActors := env.NumActivatedActors()
				if numActors != 0 {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				break
			}

			timerDelay := gcDuration * 3
			selfTimerReq = wapcutils.ScheduleSelfTimer{
				Operation: "inc",
				Payload:   nil,
				// Make the timer fires after the actor is GC'd.
				AfterMillis: int(timerDelay.Milliseconds()),
			}
			marshaledSelfTimerReq, err = json.Marshal(selfTimerReq)
			require.NoError(t, err)

			// Schedule self increment in future.
			_, err = env.
				InvokeActor(ctx, ns, "a", "test-module", "scheduleSelfTimer", marshaledSelfTimerReq, types.CreateIfNotExist{})
			require.NoError(t, err)

			// Make sure a is 0 immediately after scheduling.
			result, err = env.
				InvokeActor(ctx, ns, "a", "test-module", "getCount", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
			require.Equal(t, int64(0), getCount(t, result))

			numActors = env.NumActivatedActors()
			require.Equal(t, 1, numActors)
			// Wait for actor to be GC'd.
			for {
				numActors := env.NumActivatedActors()
				if numActors != 0 {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				break
			}

			gcTime := time.Now()

			// Make sure the timer does not reactivate the actor.
			for time.Since(gcTime) < timerDelay {
				numActors := env.NumActivatedActors()
				if numActors != 0 {
					t.Fatal("actor was reactivated by timer!")
				}

				time.Sleep(10 * time.Millisecond)
			}

		}
	}

	runWithDifferentConfigs(t, testFn, nil, false, false, gcDuration)
}

// TestInvokeActorHostFunctionDeadlockRegression is a regression test to ensure that an actor can invoke
// another actor that is not yet activated without introducing a deadlock.
func TestInvokeActorHostFunctionDeadlockRegression(t *testing.T) {
	testFn := func(t *testing.T, reg registry.Registry, env Environment) {
		ctx := context.Background()
		invokeReq := types.InvokeActorRequest{
			ActorID:   "b",
			ModuleID:  "test-module",
			Operation: "inc",
			Payload:   nil,
		}
		marshaled, err := json.Marshal(invokeReq)
		require.NoError(t, err)

		_, err = env.InvokeActor(
			ctx, "ns-1", "a", "test-module", "invokeActor", marshaled, types.CreateIfNotExist{})
		require.NoError(t, err)
	}

	runWithDifferentConfigs(t, testFn, nil, false, false, testGCActorsAfterDurationWithNoInvocations)
}

// TestHeartbeatAndSelfHealing tests the interaction between the service discovery / heartbeating system
// and the registry. It ensures that every "server" (environment) is constantly heartbeating the registry,
// that the registry will detect server's that are no longer heartbeating and reactivate the actors elsewhere,
// and that the activation/routing system can accomodate all of this.
func TestHeartbeatAndSelfHealing(t *testing.T) {
	testFn := func(t *testing.T, reg registry.Registry, env1, env2, env3 Environment) {
		var (
			ctx = context.Background()
			err error
		)
		for i := 0; i < 100; i++ {
			// Ensure we can invoke each actor from each environment. Note that just because
			// we invoke an actor on env1 first does not mean that the actor will be activated
			// on env1. The actor will be activated on whichever environment/server the Registry
			// decides and if we send the invocation to the "wrong" environment it will get
			// re-routed automatically.
			//
			// Also note that we force each environment to heartbeat manually. This is important
			// because the Registry load-balancing mechanism relies on the state provided to the
			// Registry about the server from the server heartbeats. Therefore we need to
			// heartbeat at least once after every actor is activated if we want to ensure the
			// registry is able to actually load-balance the activations evenly.
			_, err = env1.InvokeActor(ctx, "ns-1", "a", "test-module", "inc", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
			_, err = env2.InvokeActor(ctx, "ns-1", "a", "test-module", "inc", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
			_, err = env3.InvokeActor(ctx, "ns-1", "a", "test-module", "inc", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
			require.NoError(t, env1.Heartbeat())
			require.NoError(t, env2.Heartbeat())
			require.NoError(t, env3.Heartbeat())
			_, err = env1.InvokeActor(ctx, "ns-1", "b", "test-module", "inc", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
			_, err = env2.InvokeActor(ctx, "ns-1", "b", "test-module", "inc", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
			_, err = env3.InvokeActor(ctx, "ns-1", "b", "test-module", "inc", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
			require.NoError(t, env1.Heartbeat())
			require.NoError(t, env2.Heartbeat())
			require.NoError(t, env3.Heartbeat())
			_, err = env1.InvokeActor(ctx, "ns-1", "c", "test-module", "inc", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
			_, err = env2.InvokeActor(ctx, "ns-1", "c", "test-module", "inc", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
			_, err = env3.InvokeActor(ctx, "ns-1", "c", "test-module", "inc", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
			require.NoError(t, env1.Heartbeat())
			require.NoError(t, env2.Heartbeat())
			require.NoError(t, env3.Heartbeat())
		}

		// Registry load-balancing should ensure that we ended up with 1 actor in each environment
		// I.E "on each server".
		require.Equal(t, 1, env1.NumActivatedActors())
		require.Equal(t, 1, env2.NumActivatedActors())
		require.Equal(t, 1, env3.NumActivatedActors())

		require.NoError(t, env1.Close(context.Background()))
		require.NoError(t, env2.Close(context.Background()))

		// env1 and env2 have been closed (and not heartbeating) for longer than the maximum
		// heartbeat delay which means that the registry should view them as "dead". Therefore, we
		// expect that we should still be able to invoke all 3 of our actors, however, all of them
		// should end up being activated on server3 now since it is the only remaining live actor.

		for i := 0; i < 100; i++ {
			if i == 0 {
				for {
					// Spin loop until there are no more errors as function calls will fail for
					// a bit until heartbeat + activation cache expire.
					_, err = env3.InvokeActor(ctx, "ns-1", "a", "test-module", "inc", nil, types.CreateIfNotExist{})
					if err != nil {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					_, err = env3.InvokeActor(ctx, "ns-1", "b", "test-module", "inc", nil, types.CreateIfNotExist{})
					if err != nil {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					_, err = env3.InvokeActor(ctx, "ns-1", "c", "test-module", "inc", nil, types.CreateIfNotExist{})
					if err != nil {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					break
				}
				continue
			}

			_, err = env3.InvokeActor(ctx, "ns-1", "a", "test-module", "inc", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
			require.NoError(t, env3.Heartbeat())
			_, err = env3.InvokeActor(ctx, "ns-1", "b", "test-module", "inc", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
			require.NoError(t, env3.Heartbeat())
			_, err = env3.InvokeActor(ctx, "ns-1", "c", "test-module", "inc", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
			require.NoError(t, env3.Heartbeat())
		}

		// Ensure that all of our invocations above were actually served by environment3.
		require.Equal(t, 3, env3.NumActivatedActors())

		// Finally, make sure environment 3 is closed.
		require.NoError(t, env3.Close(context.Background()))
	}
	runThreeEnvironmentsWithDifferentConfigs(t, testFn)
}

// TestHeartbeatAndRebalancingWithMemory tests that the interaction between the environment
// heartbeating mechanism and the registry load balancing mechanism is able to effectively
// rebalance actors across the available nodes based on memory usage.
func TestHeartbeatAndRebalancingWithMemory(t *testing.T) {
	testFn := func(t *testing.T, reg registry.Registry, env1, env2, env3 Environment) {
		testHeartbeatAndRebalancingWithMemory(t, env1, env2, env3)
	}
	runThreeEnvironmentsWithDifferentConfigs(t, testFn)
}

func testHeartbeatAndRebalancingWithMemory(
	t *testing.T,
	env1, env2, env3 Environment,
) {
	ctx := context.Background()

	// Activate all the actors and hearbeat between each invocation so the registry can effectively load balance
	// based on actor count to start.
	for i := 0; i < 100; i++ {
		_, err := env1.InvokeActor(ctx, "ns-1", fmt.Sprintf("actor-%d", i), "test-module", "inc", nil, types.CreateIfNotExist{})
		require.NoError(t, err)
	}

	// Registry load-balancing should ensure that we ended up with an equal number of actors in each environment.
	require.True(t, env1.NumActivatedActors() == 34 || env1.NumActivatedActors() == 33)
	require.True(t, env2.NumActivatedActors() == 34 || env2.NumActivatedActors() == 33)
	require.True(t, env3.NumActivatedActors() == 34 || env3.NumActivatedActors() == 33)

	for {
		time.Sleep(10 * time.Millisecond)

		// Keep invoking all the actors to make sure they stay activated and don't get GC'd
		// for being idle.
		for i := 0; i < 100; i++ {
			// Make one of the actors look like they have really high memory usage.
			if i == 0 {
				_, err := env1.InvokeActor(
					ctx, "ns-1", "actor-0", "test-module", "setMemoryUsage",
					[]byte(fmt.Sprintf("%d", 1<<26)), types.CreateIfNotExist{})
				require.NoError(t, err)
				continue
			}

			// Only actors with memoryUsage >0 have their memory usage tracked and are eligible to be drained
			// so make sure each actor has at least 1 byte of perceived memory usage.
			_, err := env1.InvokeActor(ctx, "ns-1", fmt.Sprintf("actor-%d", i), "test-module", "setMemoryUsage", []byte("1"), types.CreateIfNotExist{})
			require.NoError(t, err)
		}

		var (
			env1Actors = env1.NumActivatedActors()
			env2Actors = env2.NumActivatedActors()
			env3Actors = env3.NumActivatedActors()
		)

		// env1 should get drained down to 1 actor as all the low memory usage actors are drained
		// away and only the high memory usage actor remains.
		if env1.NumActivatedActors() != 1 {
			continue
		}

		// Eventually env2/env3 should stabilize with roughly the same number of actors.
		delta := int(math.Abs(float64(env2Actors) - float64(env3Actors)))
		if delta > 1 {
			continue
		}

		// Finally, all actors should be activated.
		sum := env1Actors + env2Actors + env3Actors
		if sum != 100 {
			continue
		}

		// All balancing criteria have been met, we're done.
		break
	}
}

// TestReplicationRandom tests the random replication logic of the environment.
//
// This test specifically examines how actors are replicated when the ExtraReplicas
// option is set to a value greater than 0. The test verifies that the actor is
// replicated across multiple environments in a random manner.
func TestReplicationRandom(t *testing.T) {
	runThreeEnvironmentsWithDifferentConfigs(t,
		func(t *testing.T, reg registry.Registry, env1, env2, env3 Environment) {
			testReplicationRandom(t, env1, env2, env3)
		})
}

// testReplicationRandom is a test function that verifies the random replication of actors
// across multiple environments.
//
// The test logic is as follows:
//
// - Invoke an actor with the ExtraReplicas option set to 2.
// - Continuously check if the actor has been replicated in all three environments.
// - If the actor is not activated in all three environments, invoke the actor again.
// - Repeat the check until the actor is activated in all three environments or until a certain timeout is reached.
// - If the actor is not activated in all three environments within the specified time, the test fails.
func testReplicationRandom(
	t *testing.T,
	env1, env2, env3 Environment,
) {
	ctx := context.Background()

	require.Eventually(t, func() bool {
		if env1.NumActivatedActors() == 1 &&
			env2.NumActivatedActors() == 1 &&
			env3.NumActivatedActors() == 1 {
			return true
		}

		_, err := env1.InvokeActor(
			ctx, "ns-1", "actor-0", "test-module", "inc", nil,
			types.CreateIfNotExist{Options: types.ActorOptions{
				ReplicationStrategy: types.ReplicaSelectionStrategyRandom,
				ExtraReplicas:       2,
			}})
		require.NoError(t, err)

		return false
	}, time.Minute, time.Microsecond, "actor is not replicated")
}

// TestReplicationBroadcast tests the broadast replication logic of the environment.
func TestReplicationBroadcast(t *testing.T) {
	runThreeEnvironmentsWithDifferentConfigs(t,
		func(t *testing.T, reg registry.Registry, env1, env2, env3 Environment) {
			testReplicationBroadcast(t, env1, env2, env3)
		})
}

// testReplicationBroadcast is a test function that verifies the broadcast replication
// of actors across multiple environments.
//
// The test logic is as follows:
//
// - Invoke an actor with the ExtraReplicas option set to 2 and broadcast strategy.
// - Check if the actor has been replicated in all three environments.
func testReplicationBroadcast(
	t *testing.T,
	env1, env2, env3 Environment,
) {
	ctx := context.Background()
	_, err := env1.InvokeActor(
		ctx, "ns-1", "actor-0", "test-module", "inc", nil,
		types.CreateIfNotExist{Options: types.ActorOptions{
			ReplicationStrategy: types.ReplicaSelectionStrategyBroadcast,
			ExtraReplicas:       2,
		}})
	require.NoError(t, err)

	// Broadcast returns the first successful response immediately so
	// we may need to wait a bit for all the actors to be instantiated
	// in all the environments after getting the successful response.
	require.Eventually(t, func() bool {
		return env1.NumActivatedActors() == 1 &&
			env2.NumActivatedActors() == 1 &&
			env3.NumActivatedActors() == 1
	}, 15*time.Second, time.Millisecond)
}

// TestVersionStampIsHonored ensures that the interaction between the client and server
// around versionstamp coordination works by preventing the server from updating its
// internal versionstamp and ensuring that eventually RPCs start to fail because the
// server can no longer be sure it "owns" the actor and is allowed to run it.
func TestVersionStampIsHonored(t *testing.T) {
	testFn := func(t *testing.T, reg registry.Registry, env Environment) {
		ctx := context.Background()
		_, err := env.InvokeActor(
			ctx, "ns-1", "a", "test-module", "inc", nil, types.CreateIfNotExist{})
		require.NoError(t, err)

		env.freezeHeartbeatState()

		for {
			// Eventually RPCs should start to fail because the server's versionstamp will become
			// stale and it will no longer be confident that it's allowed to run RPCs for the
			// actor.
			_, err = env.InvokeActor(
				ctx, "ns-1", "a", "test-module", "inc", nil, types.CreateIfNotExist{})
			if err != nil && strings.Contains(err.Error(), "server heartbeat") {
				break
			}
			require.NoError(t, err)
			time.Sleep(100 * time.Millisecond)
		}
	}

	runWithDifferentConfigs(t, testFn, nil, true, false, testGCActorsAfterDurationWithNoInvocations)
}

// TestCustomHostFns tests the ability for users to provide custom host functions that
// can be invoked by actors.
func TestCustomHostFns(t *testing.T) {
	testFn := func(t *testing.T, reg registry.Registry, env Environment) {
		ctx := context.Background()
		result, err := env.InvokeActor(
			ctx, "ns-1", "a", "test-module", "invokeCustomHostFn", []byte("testCustomFn"), types.CreateIfNotExist{})
		require.NoError(t, err)
		require.Equal(t, []byte("ok"), result)
	}

	runWithDifferentConfigs(t, testFn, nil, false, false, testGCActorsAfterDurationWithNoInvocations)
}

// TestGoModulesRegisterTwice ensures that writing modules in pure Go and registering
// them works repeatedly and doesn't fail due to "module already exists" errors from
// the registry.
func TestGoModulesRegisterTwice(t *testing.T) {
	// Create environment and register modules.
	var (
		reg         = localregistry.NewLocalRegistry("test-server-id")
		moduleStore = newTestModuleStore()
	)
	env, err := NewEnvironment(context.Background(), "serverID1", reg, moduleStore, nil, defaultOptsGoByte)
	require.NoError(t, err)
	noErrIgnoreDupeClose(t, env.Close(context.Background()))

	// Recreate with same registry should not fail.
	env, err = NewEnvironment(context.Background(), "serverID1", reg, moduleStore, nil, defaultOptsGoByte)
	require.NoError(t, err)
	noErrIgnoreDupeClose(t, env.Close(context.Background()))
}

// TestServerVersionIsHonored ensures client-server coordination around server versions by
// blocking actor invocations if versions don't match, indicating a missed heartbeat by the
// server and loss of ownership of the actor. This reproduces the bug identified in
// https://github.com/richardartoul/nola/blob/master/proofs/stateright/activation-cache/README.md
func TestServerVersionIsHonored(t *testing.T) {
	var (
		reg         = localregistry.NewLocalRegistry("test-server-id")
		moduleStore = newTestModuleStore()
		ctx         = context.Background()
	)

	opts := defaultOptsWASM
	opts.ActivationCacheTTL = time.Second * 15
	env, err := NewEnvironment(ctx, "serverID1", reg, moduleStore, nil, opts)
	require.NoError(t, err)
	defer func() { noErrIgnoreDupeClose(t, env.Close(context.Background())) }()

	_, err = moduleStore.RegisterModule(ctx, "ns-1", "test-module", utilWasmBytes, registry.ModuleOptions{})
	require.NoError(t, err)

	_, err = env.InvokeActor(ctx, "ns-1", "a", "test-module", "inc", nil, types.CreateIfNotExist{})
	require.NoError(t, err)

	env.pauseHeartbeat()

	time.Sleep(registry.HeartbeatTTL + time.Second)

	env.resumeHeartbeat()

	require.NoError(t, env.Heartbeat())

	_, err = env.InvokeActor(ctx, "ns-1", "a", "test-module", "inc", nil, types.CreateIfNotExist{})
	require.True(
		t,
		strings.Contains(err.Error(), "server version(2) != server version from reference(1)"))
}

var (
	// Mutex is needed because multiple actors will write to it during clean shutdown
	// which will trigger the race detector in tests.
	testCleanShutdownHappenedLock sync.Mutex
	testCleanShutdownHappened     = false
)

func TestCleanShutdown(t *testing.T) {
	// Run once to ensure the actor is activated.
	testFn := func(t *testing.T, reg registry.Registry, env Environment) {
		testCleanShutdownHappenedLock.Lock()
		testCleanShutdownHappened = false
		testCleanShutdownHappenedLock.Unlock()
		_, err := env.InvokeActor(context.Background(), "ns-1", "a", "test-module", "inc", nil, types.CreateIfNotExist{})
		require.NoError(t, err)
	}

	// At this point the environment should be closed and thus the actor's clean shutdown logic should
	// have been executed. We'll verify by recreating the environment and checking the actor's state
	// to see if the clean shutdown method was invoked properly or not.

	testFnAfterClose := func(t *testing.T, reg registry.Registry, env Environment) {
		testCleanShutdownHappenedLock.Lock()
		require.True(t, testCleanShutdownHappened)
		testCleanShutdownHappenedLock.Unlock()
	}

	// skipWasm=true because we have no way to assert that the WASM was actually shutdown properly
	// without a custom host function right now.
	runWithDifferentConfigs(
		t, testFn, testFnAfterClose, false, true, testGCActorsAfterDurationWithNoInvocations)
}

func getCount(t *testing.T, v []byte) int64 {
	x, err := strconv.Atoi(string(v))
	require.NoError(t, err)
	return int64(x)
}

func runWithDifferentConfigs(
	t *testing.T,
	testFn func(t *testing.T, reg registry.Registry, env Environment),
	testFnAfterClose func(t *testing.T, reg registry.Registry, env Environment),
	skipDNS bool,
	skipWASM bool,
	gcDurationOverride time.Duration,
) {
	t.Run("wasm-local", func(t *testing.T) {
		if skipWASM {
			t.Skip("Skipping WASM...")
		}

		opts := defaultOptsWASM
		opts.GCActorsAfterDurationWithNoInvocations = gcDurationOverride

		var (
			reg         = localregistry.NewLocalRegistry("test-server-id")
			moduleStore = newTestModuleStore()
		)
		env, err := NewEnvironment(context.Background(), "serverID1", reg, moduleStore, nil, defaultOptsWASM)
		require.NoError(t, err)
		defer func() { noErrIgnoreDupeClose(t, env.Close(context.Background())) }()

		_, err = moduleStore.RegisterModule(context.Background(), "ns-1", "test-module", utilWasmBytes, registry.ModuleOptions{})
		require.NoError(t, err)
		_, err = moduleStore.RegisterModule(context.Background(), "ns-2", "test-module", utilWasmBytes, registry.ModuleOptions{})
		require.NoError(t, err)

		testFn(t, reg, env)

		err = env.Close(context.Background())
		require.NoError(t, err)
		if testFnAfterClose != nil {
			env, err = NewEnvironment(context.Background(), "serverID1", reg, moduleStore, nil, defaultOptsWASM)
			require.NoError(t, err)
			defer func() { noErrIgnoreDupeClose(t, env.Close(context.Background())) }()

			testFnAfterClose(t, reg, env)
		}
	})

	t.Run("go-local-byte", func(t *testing.T) {
		opts := defaultOptsGoByte
		opts.GCActorsAfterDurationWithNoInvocations = gcDurationOverride

		var (
			reg         = localregistry.NewLocalRegistry("test-server-id")
			moduleStore = newTestModuleStore()
		)
		defer reg.Close(context.Background())

		env, err := NewEnvironment(context.Background(), "serverID1", reg, moduleStore, nil, opts)
		require.NoError(t, err)
		defer func() { noErrIgnoreDupeClose(t, env.Close(context.Background())) }()

		env.RegisterGoModule(
			types.NamespacedIDNoType{Namespace: "ns-1", ID: "test-module"}, testModule{})
		env.RegisterGoModule(
			types.NamespacedIDNoType{Namespace: "ns-2", ID: "test-module"}, testModule{})

		testFn(t, reg, env)

		err = env.Close(context.Background())
		require.NoError(t, err)
		if testFnAfterClose != nil {
			env, err := NewEnvironment(context.Background(), "serverID1", reg, moduleStore, nil, opts)
			require.NoError(t, err)
			defer func() { noErrIgnoreDupeClose(t, env.Close(context.Background())) }()

			env.RegisterGoModule(
				types.NamespacedIDNoType{Namespace: "ns-1", ID: "test-module"}, testModule{})
			env.RegisterGoModule(
				types.NamespacedIDNoType{Namespace: "ns-2", ID: "test-module"}, testModule{})

			testFnAfterClose(t, reg, env)
		}
	})

	t.Run("go-local-stream", func(t *testing.T) {
		opts := defaultOptsGoStream
		opts.GCActorsAfterDurationWithNoInvocations = gcDurationOverride

		var (
			reg         = localregistry.NewLocalRegistry("test-server-id")
			moduleStore = newTestModuleStore()
		)
		defer reg.Close(context.Background())

		env, err := NewEnvironment(context.Background(), "serverID1", reg, moduleStore, nil, opts)
		require.NoError(t, err)
		defer func() { noErrIgnoreDupeClose(t, env.Close(context.Background())) }()

		env.RegisterGoModule(
			types.NamespacedIDNoType{Namespace: "ns-1", ID: "test-module"}, testStreamModule{})
		env.RegisterGoModule(
			types.NamespacedIDNoType{Namespace: "ns-2", ID: "test-module"}, testStreamModule{})

		testFn(t, reg, env)

		err = env.Close(context.Background())
		require.NoError(t, err)
		if testFnAfterClose != nil {
			env, err := NewEnvironment(context.Background(), "serverID1", reg, moduleStore, nil, opts)
			require.NoError(t, err)
			defer func() { noErrIgnoreDupeClose(t, env.Close(context.Background())) }()

			env.RegisterGoModule(
				types.NamespacedIDNoType{Namespace: "ns-1", ID: "test-module"}, testStreamModule{})
			env.RegisterGoModule(
				types.NamespacedIDNoType{Namespace: "ns-2", ID: "test-module"}, testStreamModule{})

			testFnAfterClose(t, reg, env)
		}

		// Ensure that the stream interface is used at least once because it depends on some runtime
		// type assertions that could easily be messed up / never happen.
		streamInterfaceWasCalledMutex.Lock()
		defer streamInterfaceWasCalledMutex.Unlock()
		require.True(t, streamInterfaceWasCalled)
	})

	if !skipDNS {
		t.Run("go-dns", func(t *testing.T) {
			opts := defaultOptsGoDNS
			opts.GCActorsAfterDurationWithNoInvocations = gcDurationOverride

			env, reg, err := NewTestDNSRegistryEnvironment(context.Background(), opts)
			require.NoError(t, err)
			defer func() { noErrIgnoreDupeClose(t, env.Close(context.Background())) }()

			env.RegisterGoModule(
				types.NamespacedIDNoType{Namespace: "ns-1", ID: "test-module"}, testStreamModule{})
			env.RegisterGoModule(
				types.NamespacedIDNoType{Namespace: "ns-2", ID: "test-module"}, testStreamModule{})

			testFn(t, reg, env)
		})
	}
}

func runThreeEnvironmentsWithDifferentConfigs(
	t *testing.T,
	testFn func(t *testing.T, reg registry.Registry, env1, env2, env3 Environment),
) {
	t.Run("go module", func(t *testing.T) {
		var (
			reg = localregistry.NewLocalRegistryWithOptions(
				"test-server-id",
				registry.KVRegistryOptions{
					RebalanceMemoryThreshold: 1 << 24,
				})
			moduleStore = newTestModuleStore()
			ctx         = context.Background()
		)
		defer reg.Close(context.Background())

		// Create 3 environments backed by the same registry to simulate 3 different servers. Each environment
		// needs its own port so it looks unique.
		opts1 := defaultOptsGoByte
		opts1.Discovery.Port = 1
		env1, err := NewEnvironment(ctx, "serverID1", reg, moduleStore, nil, opts1)
		require.NoError(t, err)
		defer env1.Close(context.Background())
		env1.RegisterGoModule(
			types.NamespacedIDNoType{Namespace: "ns-1", ID: "test-module"}, testModule{})

		opts2 := defaultOptsGoByte
		opts2.Discovery.Port = 2
		env2, err := NewEnvironment(ctx, "serverID2", reg, moduleStore, nil, opts2)
		require.NoError(t, err)
		defer env2.Close(context.Background())
		env2.RegisterGoModule(
			types.NamespacedIDNoType{Namespace: "ns-1", ID: "test-module"}, testModule{})

		opts3 := defaultOptsGoByte
		opts3.Discovery.Port = 3
		env3, err := NewEnvironment(ctx, "serverID3", reg, moduleStore, nil, opts3)
		require.NoError(t, err)
		defer env3.Close(context.Background())
		env3.RegisterGoModule(
			types.NamespacedIDNoType{Namespace: "ns-1", ID: "test-module"}, testModule{})
		testFn(t, reg, env1, env2, env3)
	})

	t.Run("WASM module", func(t *testing.T) {
		var (
			reg = localregistry.NewLocalRegistryWithOptions(
				"test-server-id",
				registry.KVRegistryOptions{
					RebalanceMemoryThreshold: 1 << 24,
				})
			moduleStore = newTestModuleStore()
			ctx         = context.Background()
		)
		_, err := moduleStore.RegisterModule(ctx, "ns-1", "test-module", utilWasmBytes, registry.ModuleOptions{})
		require.NoError(t, err)

		// Create 3 environments backed by the same registry to simulate 3 different servers. Each environment
		// needs its own port so it looks unique.
		opts1 := defaultOptsWASM
		opts1.Discovery.Port = 1
		env1, err := NewEnvironment(ctx, "serverID1", reg, moduleStore, nil, opts1)
		require.NoError(t, err)
		defer env1.Close(context.Background())

		opts2 := defaultOptsWASM
		opts2.Discovery.Port = 2
		env2, err := NewEnvironment(ctx, "serverID2", reg, moduleStore, nil, opts2)
		require.NoError(t, err)
		defer env2.Close(context.Background())

		opts3 := defaultOptsWASM
		opts3.Discovery.Port = 3
		env3, err := NewEnvironment(ctx, "serverID3", reg, moduleStore, nil, opts3)
		require.NoError(t, err)
		defer env3.Close(context.Background())
		testFn(t, reg, env1, env2, env3)
	})
}

type testModule struct {
}

func newTestModuleStore() registry.ModuleStore {
	return localregistry.NewLocalRegistry("test-server-id").(registry.ModuleStore)
}

func (tm testModule) Instantiate(
	ctx context.Context,
	reference types.ActorReferenceVirtual,
	payload []byte,
	host HostCapabilities,
) (Actor, error) {
	return &testActor{
		host:               host,
		instantiatePayload: payload,
	}, nil
}

func (tm testModule) Close(ctx context.Context) error {
	return nil
}

type testActor struct {
	host HostCapabilities

	count              int
	numInvocations     int
	startupWasCalled   bool
	shutdownWasCalled  bool
	memUsage           int
	instantiatePayload []byte
}

func (ta *testActor) MemoryUsageBytes() int {
	return ta.memUsage
}

func (ta *testActor) Invoke(
	ctx context.Context,
	operation string,
	payload []byte,
) ([]byte, error) {
	defer func() { ta.numInvocations++ }()

	switch operation {
	case wapcutils.StartupOperationName:
		ta.startupWasCalled = true
		return nil, nil
	case wapcutils.ShutdownOperationName:
		ta.shutdownWasCalled = true
		testCleanShutdownHappenedLock.Lock()
		testCleanShutdownHappened = true
		testCleanShutdownHappenedLock.Unlock()
		return nil, nil
	case "getShutdownValue":
		return []byte(strconv.FormatBool(ta.shutdownWasCalled)), nil
	case "getInstantiatePayload":
		return ta.instantiatePayload, nil
	case "inc":
		ta.count++
		return []byte(strconv.Itoa(ta.count)), nil
	case "getCount":
		return []byte(strconv.Itoa(ta.count)), nil
	case "getStartupWasCalled":
		if ta.startupWasCalled {
			return []byte("true"), nil
		}
		return []byte("false"), nil
	case "invokeActor":
		var req types.InvokeActorRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, err
		}
		return ta.host.InvokeActor(ctx, req)
	case "scheduleSelfTimer":
		var req wapcutils.ScheduleSelfTimer
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, err
		}
		err := ta.host.ScheduleSelfTimer(ctx, req)
		return nil, err
	case "invokeCustomHostFn":
		return ta.host.CustomFn(ctx, string(payload), payload)
	case "setMemoryUsage":
		memUsage, err := strconv.ParseInt(string(payload), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing memUsage in payload: %w", err)
		}
		ta.memUsage = int(memUsage)
		return nil, nil
	default:
		return nil, fmt.Errorf("testActor: unhandled operation: %s", operation)
	}
}

func (ta testActor) Close(ctx context.Context) error {
	return nil
}

// Same as testModule but implements InvokeStream in addition to Invoke.
type testStreamModule struct {
}

func (tm testStreamModule) Instantiate(
	ctx context.Context,
	reference types.ActorReferenceVirtual,
	payload []byte,
	host HostCapabilities,
) (Actor, error) {
	streamInterfaceWasCalledMutex.Lock()
	defer streamInterfaceWasCalledMutex.Unlock()
	streamInterfaceWasCalled = true
	return &testStreamActor{
		a: &testActor{
			host:               host,
			instantiatePayload: payload,
		},
	}, nil
}

func (tm testStreamModule) Close(ctx context.Context) error {
	return nil
}

// Same as testActor, but implement InvokeStream in addition to Invoke.
type testStreamActor struct {
	a ActorBytes
}

func (ta *testStreamActor) MemoryUsageBytes() int {
	return ta.a.MemoryUsageBytes()
}

func (ta *testStreamActor) InvokeStream(
	ctx context.Context,
	operation string,
	payload []byte,
) (io.ReadCloser, error) {
	resp, err := ta.a.Invoke(ctx, operation, payload)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewBuffer(resp)), nil
}

func (ta *testStreamActor) Close(ctx context.Context) error {
	return nil
}

// noErrIgnoreDupeClose asserts that the error is nil or an "environment is
// closed error".
func noErrIgnoreDupeClose(t *testing.T, err error) {
	if err != nil && strings.Contains(err.Error(), "environment is closed") {
		return
	}

	require.NoError(t, err)
}
