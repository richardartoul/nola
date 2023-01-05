package virtual

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/wapcutils"

	"github.com/stretchr/testify/require"
)

func BenchmarkLocalInvoke(b *testing.B) {
	reg := registry.NewLocalRegistry()

	env, err := NewEnvironment(context.Background(), "serverID1", reg, defaultOpts)
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

func BenchmarkLocalCreateActor(b *testing.B) {
	b.Skip("Skip this benchmark for now since its not interesting with a fake in-memory registry implementation")

	reg := registry.NewLocalRegistry()
	env, err := NewEnvironment(context.Background(), "serverID1", reg, defaultOpts)
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

func BenchmarkLocalCreateThenInvokeActor(b *testing.B) {
	reg := registry.NewLocalRegistry()
	env, err := NewEnvironment(context.Background(), "serverID1", reg, defaultOpts)
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

func BenchmarkLocalActorToActorCommunication(b *testing.B) {
	reg := registry.NewLocalRegistry()
	env, err := NewEnvironment(context.Background(), "serverID1", reg, defaultOpts)
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

// Can't use the micro-benchmarking framework because we need concurrency.
func TestBenchmarkFoundationRegistryInvoke(t *testing.T) {
	testSimpleBench(t, 10, 25*time.Microsecond, 15*time.Second)
}

func testSimpleBench(
	t *testing.T,
	numActors int,
	invokeEvery time.Duration,
	benchDuration time.Duration,
) {
	// Uncomment to run.
	t.Skip()

	reg, err := registry.NewFoundationDBRegistry("")
	require.NoError(t, err)
	require.NoError(t, reg.UnsafeWipeAll())

	env, err := NewEnvironment(context.Background(), "serverID1", reg, defaultOpts)
	require.NoError(t, err)
	defer env.Close()

	_, err = reg.RegisterModule(context.Background(), "bench-ns", "test-module", utilWasmBytes, registry.ModuleOptions{})
	require.NoError(t, err)

	for i := 0; i < numActors; i++ {
		actorID := fmt.Sprintf("%d", i)
		_, err = reg.CreateActor(context.Background(), "bench-ns", actorID, "test-module", registry.ActorOptions{})
		require.NoError(t, err)

		_, err = env.Invoke(context.Background(), "bench-ns", actorID, "incFast", nil)
		require.NoError(t, err)
	}

	sketch, err := ddsketch.NewDefaultDDSketch(0.01)
	require.NoError(t, err)

	var (
		ctx, cc    = context.WithCancel(context.Background())
		wg         sync.WaitGroup
		benchState = &benchState{
			invokeLatency: sketch,
		}
		invokeTicker = time.NewTicker(invokeEvery)
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		rand := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 1; ; i++ {
			i := i // Capture for async goroutine.
			actorID := fmt.Sprintf("%d", rand.Intn(numActors))
			select {
			case <-invokeTicker.C:
				wg.Add(1)
				go func() {
					defer wg.Done()

					start := time.Now()
					_, err = env.Invoke(ctx, "bench-ns", actorID, "incFast", nil)
					if err != nil {
						panic(err)
					}

					benchState.trackInvokeLatency(time.Since(start))
					if i%100 == 0 {
						benchState.setNumInvokes(i)
					}
				}()
			case <-ctx.Done():
				benchState.setNumInvokes(i)
				return
			}
		}
	}()

	time.Sleep(benchDuration)
	invokeTicker.Stop()
	cc()
	wg.Wait()

	fmt.Println("Inputs")
	fmt.Println("    invokeEvery", invokeEvery)
	fmt.Println("Results")
	fmt.Println("    numInvokes", benchState.getNumInvokes())
	fmt.Println("    invoke/s", float64(benchState.getNumInvokes())/benchDuration.Seconds())
	fmt.Println("    median latency (puts)", getQuantile(t, benchState.invokeLatency, 0.5))
	fmt.Println("    p95 latency (puts)", getQuantile(t, benchState.invokeLatency, 0.95), "ms")
	fmt.Println("    p99 latency (puts)", getQuantile(t, benchState.invokeLatency, 0.99), "ms")
	fmt.Println("    p99.9 latency (puts)", getQuantile(t, benchState.invokeLatency, 0.999), "ms")

	t.Fail() // Fail so it prints output.
}

func getQuantile(t *testing.T, sketch *ddsketch.DDSketch, q float64) float64 {
	quantile, err := sketch.GetValueAtQuantile(q)
	require.NoError(t, err)
	return quantile
}

type benchState struct {
	sync.RWMutex

	numInvokes    int
	invokeLatency *ddsketch.DDSketch
}

func (b *benchState) setNumInvokes(x int) {
	b.Lock()
	defer b.Unlock()

	b.numInvokes = x
}

func (b *benchState) getNumInvokes() int {
	b.RLock()
	defer b.RUnlock()

	return b.numInvokes
}

func (b *benchState) trackInvokeLatency(x time.Duration) {
	b.Lock()
	defer b.Unlock()

	b.invokeLatency.Add(float64(x.Milliseconds()))
}
