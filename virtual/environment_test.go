package virtual

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"testing"

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
	env, err := NewEnvironment(reg)
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
	env, err := NewEnvironment(reg)
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

		env, err := NewEnvironment(reg)
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
	env, err := NewEnvironment(reg)
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
	env, err := NewEnvironment(reg)
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
	env, err := NewEnvironment(reg)
	require.NoError(t, err)
	defer env.Close()

	ctx := context.Background()

	_, err = reg.RegisterModule(ctx, "bench-ns", "test-module", utilWasmBytes, registry.ModuleOptions{})
	require.NoError(t, err)

	_, err = reg.CreateActor(ctx, "bench-ns", "a", "test-module", registry.ActorOptions{})
	require.NoError(t, err)
	_, err = reg.CreateActor(ctx, "bench-ns", "b", "test-module", registry.ActorOptions{})
	require.NoError(t, err)

	invokeReq := wapcutils.InvokeActorRequest{
		ActorID:   "b",
		Operation: "inc",
		Payload:   nil,
	}
	marshaled, err := json.Marshal(invokeReq)
	require.NoError(t, err)

	_, err = env.Invoke(ctx, "bench-ns", "a", "invokeActor", marshaled)
	require.NoError(t, err)
}

func getCount(t *testing.T, v []byte) int64 {
	x, err := strconv.Atoi(string(v))
	require.NoError(t, err)
	return int64(x)
}
