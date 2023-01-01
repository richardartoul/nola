package virtual

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"testing"
	"time"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/wapcutils"

	"github.com/stretchr/testify/require"
)

var utilWasmBytes []byte

func init() {
	fBytes, err := ioutil.ReadFile("../testdata/tinygo/util/main.wasm")
	if err != nil {
		panic(err)
	}
	utilWasmBytes = fBytes
}

// TODO: Need a good concurrency test that spans a bunch of goroutine and
//       spams registry operations + invocations.

// TestSimple is a basic sanity test that verifies the most basic flow.
func TestSimple(t *testing.T) {
	reg := registry.NewLocal()
	env, err := NewEnvironment(context.Background(), "serverID1", reg)
	require.NoError(t, err)
	defer env.Close()

	ctx := context.Background()

	for _, ns := range []string{"ns-1", "ns-2"} {
		// Can't invoke because neither module nor actor exist.
		_, err = env.Invoke(ctx, ns, "a", "inc", nil)
		require.Error(t, err)

		// Create module.
		_, err = reg.RegisterModule(ctx, ns, "test-module", utilWasmBytes, registry.ModuleOptions{})
		require.NoError(t, err)

		// Can't invoke because actor doesn't exist yet.
		_, err = env.Invoke(ctx, ns, "a", "inc", nil)
		require.Error(t, err)

		// Create actor.
		_, err = reg.CreateActor(ctx, ns, "a", "test-module", registry.ActorOptions{})
		require.NoError(t, err)

		for i := 0; i < 100; i++ {
			// Invoke should work now.
			result, err := env.Invoke(ctx, ns, "a", "inc", nil)
			require.NoError(t, err)
			require.Equal(t, int64(i+1), getCount(t, result))
		}
	}
}

// TestGenerationCountIncInvalidatesActivation ensures that the registry returning a higher
// generation count will cause the environment to invalidate existing activations and recreate
// them as needed.
func TestGenerationCountIncInvalidatesActivation(t *testing.T) {
	reg := registry.NewLocal()
	env, err := NewEnvironment(context.Background(), "serverID1", reg)
	require.NoError(t, err)
	defer env.Close()

	ctx := context.Background()

	for _, ns := range []string{"ns-1", "ns-2"} {
		_, err = reg.RegisterModule(ctx, ns, "test-module", utilWasmBytes, registry.ModuleOptions{})
		require.NoError(t, err)

		_, err = reg.CreateActor(ctx, ns, "a", "test-module", registry.ActorOptions{})
		require.NoError(t, err)

		// Build some state.
		for i := 0; i < 100; i++ {
			result, err := env.Invoke(ctx, ns, "a", "inc", nil)
			require.NoError(t, err)
			require.Equal(t, int64(i+1), getCount(t, result))
		}

		// Increment the generation which should cause the next invocation to recreate the actor
		// activation from scratch and reset the internal counter back to 0.
		reg.IncGeneration(ctx, ns, "a")

		for i := 0; i < 100; i++ {
			result, err := env.Invoke(ctx, ns, "a", "inc", nil)
			require.NoError(t, err)
			require.Equal(t, int64(i+1), getCount(t, result))
		}
	}
}

// TestKVHostFunctions tests whether the KV interfaces from the registry can be used properly as host functions
// in the actor WASM module.
func TestKVHostFunctions(t *testing.T) {
	var (
		reg   = registry.NewLocal()
		count = 0
	)
	testFn := func() {
		defer func() {
			count++
		}()

		env, err := NewEnvironment(context.Background(), "serverID1", reg)
		require.NoError(t, err)
		defer env.Close()

		ctx := context.Background()
		for _, ns := range []string{"ns-1", "ns-2"} {
			if count == 0 {
				_, err = reg.RegisterModule(ctx, ns, "test-module", utilWasmBytes, registry.ModuleOptions{})
				require.NoError(t, err)

				_, err = reg.CreateActor(ctx, ns, "a", "test-module", registry.ActorOptions{})
				require.NoError(t, err)

				for i := 0; i < 100; i++ {
					_, err := env.Invoke(ctx, ns, "a", "inc", nil)
					require.NoError(t, err)

					// Write the current count to a key.
					key := []byte(fmt.Sprintf("key-%d", i))
					_, err = env.Invoke(ctx, ns, "a", "kvPutCount", key)
					require.NoError(t, err)

					// Read the key back and make sure the value is == the count
					payload, err := env.Invoke(ctx, ns, "a", "kvGet", key)
					require.NoError(t, err)
					val := getCount(t, payload)
					require.Equal(t, int64(i+1), val)

					if i > 0 {
						key := []byte(fmt.Sprintf("key-%d", i-1))
						payload, err := env.Invoke(ctx, ns, "a", "kvGet", key)
						require.NoError(t, err)
						val := getCount(t, payload)
						require.Equal(t, int64(i), val)
					}
				}
			}

			// Ensure all previous KV are still readable.
			for i := 0; i < 100; i++ {
				key := []byte(fmt.Sprintf("key-%d", i))
				payload, err := env.Invoke(ctx, ns, "a", "kvGet", key)
				require.NoError(t, err)
				val := getCount(t, payload)
				require.Equal(t, int64(i+1), val)
			}
		}
	}

	// Run the test twice with two different environments, but the same registry
	// to simulate a node restarting and being re-initialized with the same registry
	// to ensure the KV operations are durable if the KV itself is.
	testFn()
	testFn()
}

// TestCreateActorHostFunction tests whether the create actor host function can be used
// by the WASM module to create new actors on demand. In other words, this test ensures
// that actors can create new actors.
func TestCreateActorHostFunction(t *testing.T) {
	reg := registry.NewLocal()
	env, err := NewEnvironment(context.Background(), "serverID1", reg)
	require.NoError(t, err)
	defer env.Close()

	ctx := context.Background()

	for _, ns := range []string{"ns-1", "ns-2"} {
		_, err = reg.RegisterModule(ctx, ns, "test-module", utilWasmBytes, registry.ModuleOptions{})
		require.NoError(t, err)

		_, err = reg.CreateActor(ctx, ns, "a", "test-module", registry.ActorOptions{})
		require.NoError(t, err)

		// Succeeds because actor exists.
		_, err := env.Invoke(ctx, ns, "a", "echo", nil)
		require.NoError(t, err)

		// Fails because actor does not exist.
		_, err = env.Invoke(ctx, ns, "b", "inc", nil)
		require.Error(t, err)

		// Create a new actor b by calling fork() on a, not by creating it ourselves.
		_, err = env.Invoke(ctx, ns, "a", "fork", []byte("b"))
		require.NoError(t, err)

		// Should succeed now that actor a has created actor b.
		_, err = env.Invoke(ctx, ns, "b", "echo", nil)
		require.NoError(t, err)

		for _, actor := range []string{"a", "b"} {
			for i := 0; i < 100; i++ {
				_, err := env.Invoke(ctx, ns, actor, "inc", nil)
				require.NoError(t, err)

				// Write the current count to a key.
				key := []byte(fmt.Sprintf("key-%d", i))
				_, err = env.Invoke(ctx, ns, actor, "kvPutCount", key)
				require.NoError(t, err)

				// Read the key back and make sure the value is == the count
				payload, err := env.Invoke(ctx, ns, actor, "kvGet", key)
				require.NoError(t, err)
				val := getCount(t, payload)
				require.Equal(t, int64(i+1), val)
			}
		}

	}
}

// TestInvokeActorHostFunction tests whether the invoke actor host function can be used
// by the WASM module to invoke operations on other actors on demand. In other words, this
// test ensures that actors can communicate with other actors.
func TestInvokeActorHostFunction(t *testing.T) {
	reg := registry.NewLocal()
	env, err := NewEnvironment(context.Background(), "serverID1", reg)
	require.NoError(t, err)
	defer env.Close()

	ctx := context.Background()

	for _, ns := range []string{"ns-1", "ns-2"} {
		_, err = reg.RegisterModule(ctx, ns, "test-module", utilWasmBytes, registry.ModuleOptions{})
		require.NoError(t, err)

		// Create an actor, then immediately fork it so we have two actors.
		_, err = reg.CreateActor(ctx, ns, "a", "test-module", registry.ActorOptions{})
		require.NoError(t, err)

		_, err = env.Invoke(ctx, ns, "a", "fork", []byte("b"))
		require.NoError(t, err)

		// Ensure actor a can communicate with actor b.
		invokeReq := wapcutils.InvokeActorRequest{
			ActorID:   "b",
			Operation: "inc",
			Payload:   nil,
		}
		marshaled, err := json.Marshal(invokeReq)
		require.NoError(t, err)
		_, err = env.Invoke(ctx, ns, "a", "invokeActor", marshaled)
		require.NoError(t, err)

		// Ensure actor b can communicate with actor a.
		invokeReq = wapcutils.InvokeActorRequest{
			ActorID:   "a",
			Operation: "inc",
			Payload:   nil,
		}
		marshaled, err = json.Marshal(invokeReq)
		require.NoError(t, err)
		_, err = env.Invoke(ctx, ns, "b", "invokeActor", marshaled)
		require.NoError(t, err)

		// Ensure both actor's state was actually updated and they can request
		// each other's state.
		invokeReq = wapcutils.InvokeActorRequest{
			ActorID:   "b",
			Operation: "getCount",
			Payload:   nil,
		}
		marshaled, err = json.Marshal(invokeReq)
		require.NoError(t, err)
		result, err := env.Invoke(ctx, ns, "a", "invokeActor", marshaled)
		require.NoError(t, err)
		require.Equal(t, int64(1), getCount(t, result))

		invokeReq = wapcutils.InvokeActorRequest{
			ActorID:   "a",
			Operation: "getCount",
			Payload:   nil,
		}
		marshaled, err = json.Marshal(invokeReq)
		require.NoError(t, err)
		result, err = env.Invoke(ctx, ns, "b", "invokeActor", marshaled)
		require.NoError(t, err)
		require.Equal(t, int64(1), getCount(t, result))
	}
}

// TestInvokeActorHostFunctionDeadlockRegression is a regression test to ensure that an actor can invoke
// another actor that is not yet activated without introducing a deadlock.
func TestInvokeActorHostFunctionDeadlockRegression(t *testing.T) {
	reg := registry.NewLocal()
	env, err := NewEnvironment(context.Background(), "serverID1", reg)
	require.NoError(t, err)
	defer env.Close()

	ctx := context.Background()

	_, err = reg.RegisterModule(ctx, "ns-1", "test-module", utilWasmBytes, registry.ModuleOptions{})
	require.NoError(t, err)

	_, err = reg.CreateActor(ctx, "ns-1", "a", "test-module", registry.ActorOptions{})
	require.NoError(t, err)
	_, err = reg.CreateActor(ctx, "ns-1", "b", "test-module", registry.ActorOptions{})
	require.NoError(t, err)

	invokeReq := wapcutils.InvokeActorRequest{
		ActorID:   "b",
		Operation: "inc",
		Payload:   nil,
	}
	marshaled, err := json.Marshal(invokeReq)
	require.NoError(t, err)

	_, err = env.Invoke(ctx, "ns-1", "a", "invokeActor", marshaled)
	require.NoError(t, err)
}

// TestHeartbeatAndSelfHealing tests the interaction between the service discovery / heartbeating system
// and the registry. It ensures that every "server" (environment) is constantly heartbeating the registry,
// that the registry will detect server's that are no longer heartbeating and reactivate the actors elsewhere,
// and that the activation/routing system can accomodate all of this.
func TestHeartbeatAndSelfHealing(t *testing.T) {
	var (
		reg = registry.NewLocal()
		ctx = context.Background()
	)
	// TODO: Ensure all close.
	// Create 3 environments backed by the same registry to simulate 3 different servers.
	env1, err := NewEnvironment(ctx, "serverID1", reg)
	require.NoError(t, err)
	env2, err := NewEnvironment(ctx, "serverID2", reg)
	require.NoError(t, err)
	env3, err := NewEnvironment(ctx, "serverID3", reg)
	require.NoError(t, err)

	_, err = reg.RegisterModule(ctx, "ns-1", "test-module", utilWasmBytes, registry.ModuleOptions{})
	require.NoError(t, err)

	// Create 3 different actors because we want to end up with at least one actor on each
	// server to test the ability to "migrate" the actor's activation from one server to
	// another.
	_, err = reg.CreateActor(ctx, "ns-1", "a", "test-module", registry.ActorOptions{})
	require.NoError(t, err)
	_, err = reg.CreateActor(ctx, "ns-1", "b", "test-module", registry.ActorOptions{})
	require.NoError(t, err)
	_, err = reg.CreateActor(ctx, "ns-1", "c", "test-module", registry.ActorOptions{})
	require.NoError(t, err)

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
		_, err = env1.Invoke(ctx, "ns-1", "a", "inc", nil)
		require.NoError(t, err)
		_, err = env2.Invoke(ctx, "ns-1", "a", "inc", nil)
		require.NoError(t, err)
		_, err = env3.Invoke(ctx, "ns-1", "a", "inc", nil)
		require.NoError(t, err)
		require.NoError(t, env1.heartbeat())
		require.NoError(t, env2.heartbeat())
		require.NoError(t, env3.heartbeat())
		_, err = env1.Invoke(ctx, "ns-1", "b", "inc", nil)
		require.NoError(t, err)
		_, err = env2.Invoke(ctx, "ns-1", "b", "inc", nil)
		require.NoError(t, err)
		_, err = env3.Invoke(ctx, "ns-1", "b", "inc", nil)
		require.NoError(t, err)
		require.NoError(t, env1.heartbeat())
		require.NoError(t, env2.heartbeat())
		require.NoError(t, env3.heartbeat())
		_, err = env1.Invoke(ctx, "ns-1", "c", "inc", nil)
		require.NoError(t, err)
		_, err = env2.Invoke(ctx, "ns-1", "c", "inc", nil)
		require.NoError(t, err)
		_, err = env3.Invoke(ctx, "ns-1", "c", "inc", nil)
		require.NoError(t, err)
		require.NoError(t, env1.heartbeat())
		require.NoError(t, env2.heartbeat())
		require.NoError(t, env3.heartbeat())
	}

	// Registry load-balancing should ensure that we ended up with 1 actor in each environment
	// I.E "on each server".
	require.Equal(t, 1, env1.numActivatedActors())
	require.Equal(t, 1, env2.numActivatedActors())
	require.Equal(t, 1, env3.numActivatedActors())

	// TODO: Sleeps in tests are bad, but I'm lazy to inject a clock right now and deal
	//       with all of that.
	require.NoError(t, env1.Close())
	require.NoError(t, env2.Close())
	time.Sleep(registry.MaxHeartbeatDelay + time.Second)

	// env1 and env2 have been closed (and not heartbeating) for longer than the maximum
	// heartbeat delay which means that the registry should view them as "dead". Therefore, we
	// expect that we should still be able to invoke all 3 of our actors, however, all of them
	// should end up being activated on server3 now since it is the only remaining live actor.

	// Quickly make sure the registry has an up to date view of env3.
	require.NoError(t, env3.heartbeat())
	for i := 0; i < 100; i++ {
		_, err = env3.Invoke(ctx, "ns-1", "a", "inc", nil)
		require.NoError(t, err)
		require.NoError(t, env3.heartbeat())
		_, err = env3.Invoke(ctx, "ns-1", "b", "inc", nil)
		require.NoError(t, err)
		require.NoError(t, env3.heartbeat())
		_, err = env3.Invoke(ctx, "ns-1", "c", "inc", nil)
		require.NoError(t, err)
		require.NoError(t, env3.heartbeat())
	}

	// Ensure that all of our invocations above were actually served by environment3.
	require.Equal(t, 3, env3.numActivatedActors())
}

func getCount(t *testing.T, v []byte) int64 {
	x, err := strconv.Atoi(string(v))
	require.NoError(t, err)
	return int64(x)
}
