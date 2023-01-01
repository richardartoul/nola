package virtual

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/wapcutils"

	"github.com/stretchr/testify/require"
)

func BenchmarkInvoke(b *testing.B) {
	reg := registry.NewLocal()
	env, err := NewEnvironment(context.Background(), "serverID1", reg)
	require.NoError(b, err)
	defer env.Close()

	ctx := context.Background()

	_, err = reg.RegisterModule(ctx, "bench-ns", "test-module", utilWasmBytes, registry.ModuleOptions{})
	require.NoError(b, err)

	_, err = reg.CreateActor(ctx, "bench-ns", "a", "test-module", registry.ActorOptions{})
	require.NoError(b, err)

	defer reportOpsPerSecond(b)()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = env.Invoke(ctx, "bench-ns", "a", "incFast", nil)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkCreateActor(b *testing.B) {
	b.Skip("Skip this benchmark for now since its not interesting with a fake in-memory registry implementation")

	reg := registry.NewLocal()
	env, err := NewEnvironment(context.Background(), "serverID1", reg)
	require.NoError(b, err)
	defer env.Close()

	ctx := context.Background()

	_, err = reg.RegisterModule(ctx, "bench-ns", "test-module", utilWasmBytes, registry.ModuleOptions{})
	require.NoError(b, err)

	defer reportOpsPerSecond(b)()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = reg.CreateActor(ctx, "bench-ns", fmt.Sprintf("%d", i), "test-module", registry.ActorOptions{})
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkCreateThenInvokeActor(b *testing.B) {
	reg := registry.NewLocal()
	env, err := NewEnvironment(context.Background(), "serverID1", reg)
	require.NoError(b, err)
	defer env.Close()

	ctx := context.Background()

	_, err = reg.RegisterModule(ctx, "bench-ns", "test-module", utilWasmBytes, registry.ModuleOptions{})
	require.NoError(b, err)

	defer reportOpsPerSecond(b)()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		actorID := fmt.Sprintf("%d", i)
		_, err = reg.CreateActor(ctx, "bench-ns", actorID, "test-module", registry.ActorOptions{})
		if err != nil {
			panic(err)
		}
		_, err = env.Invoke(ctx, "bench-ns", actorID, "incFast", nil)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkActorToActorCommunication(b *testing.B) {
	reg := registry.NewLocal()
	env, err := NewEnvironment(context.Background(), "serverID1", reg)
	require.NoError(b, err)
	defer env.Close()

	ctx := context.Background()

	_, err = reg.RegisterModule(ctx, "bench-ns", "test-module", utilWasmBytes, registry.ModuleOptions{})
	require.NoError(b, err)

	_, err = reg.CreateActor(ctx, "bench-ns", "a", "test-module", registry.ActorOptions{})
	require.NoError(b, err)
	_, err = reg.CreateActor(ctx, "bench-ns", "b", "test-module", registry.ActorOptions{})
	require.NoError(b, err)

	invokeReq := wapcutils.InvokeActorRequest{
		ActorID:   "b",
		Operation: "incFast",
		Payload:   nil,
	}
	marshaled, err := json.Marshal(invokeReq)
	require.NoError(b, err)

	defer reportOpsPerSecond(b)()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = env.Invoke(ctx, "bench-ns", "a", "invokeActor", marshaled)
		if err != nil {
			panic(err)
		}
	}
}

func reportOpsPerSecond(b *testing.B) func() {
	start := time.Now()
	return func() {
		elapsedSeconds := time.Since(start).Seconds()
		b.ReportMetric(float64(b.N)/(elapsedSeconds), "ops/s")
	}
}
