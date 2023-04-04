package benchmarks

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/richardartoul/nola/virtual"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/registry/fdbregistry"
	"github.com/richardartoul/nola/virtual/registry/localregistry"
	"github.com/richardartoul/nola/virtual/types"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/stretchr/testify/require"
)

var (
	utilWasmBytes   []byte
	defaultOptsWASM = virtual.EnvironmentOptions{
		Discovery: virtual.DiscoveryOptions{
			DiscoveryType: virtual.DiscoveryTypeLocalHost,
		},
		CustomHostFns: map[string]func([]byte) ([]byte, error){
			"testCustomFn": func([]byte) ([]byte, error) {
				return []byte("ok"), nil
			},
		},
	}
)

func init() {
	fBytes, err := ioutil.ReadFile("../../../testdata/tinygo/util/main.wasm")
	if err != nil {
		panic(err)
	}
	utilWasmBytes = fBytes
}

func BenchmarkLocalInvokeActor(b *testing.B) {
	reg := localregistry.NewLocalRegistry()
	benchmarkInvokeActor(b, reg)
}

func BenchmarkLocalInvokeWorker(b *testing.B) {
	reg := localregistry.NewLocalRegistry()
	benchmarkInvokeWorker(b, reg)
}

func BenchmarkFoundationDBRegistryInvokeActor(b *testing.B) {
	reg, err := fdbregistry.NewFoundationDBRegistry("")
	require.NoError(b, err)
	require.NoError(b, reg.UnsafeWipeAll())

	benchmarkInvokeActor(b, reg)
}

func BenchmarkFoundationDBRegistryInvokeWorker(b *testing.B) {
	reg, err := fdbregistry.NewFoundationDBRegistry("")
	require.NoError(b, err)
	require.NoError(b, reg.UnsafeWipeAll())

	benchmarkInvokeWorker(b, reg)
}

func benchmarkInvokeActor(b *testing.B, reg registry.Registry) {
	env, err := virtual.NewEnvironment(context.Background(), "serverID1", reg, nil, defaultOptsWASM)
	require.NoError(b, err)
	defer env.Close(context.Background())

	ctx := context.Background()

	_, err = reg.RegisterModule(ctx, "bench-ns", "test-module", utilWasmBytes, registry.ModuleOptions{})
	require.NoError(b, err)

	defer reportOpsPerSecond(b)()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = env.InvokeActor(ctx, "bench-ns", "a", "test-module", "incFast", nil, types.CreateIfNotExist{})
		if err != nil {
			panic(err)
		}
	}
}

func benchmarkInvokeWorker(b *testing.B, reg registry.Registry) {
	env, err := virtual.NewEnvironment(context.Background(), "serverID1", reg, nil, defaultOptsWASM)
	require.NoError(b, err)
	defer env.Close(context.Background())

	ctx := context.Background()

	_, err = reg.RegisterModule(ctx, "bench-ns", "test-module", utilWasmBytes, registry.ModuleOptions{})
	require.NoError(b, err)

	defer reportOpsPerSecond(b)()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = env.InvokeWorker(
			ctx, "bench-ns", "test-module",
			"incFast", nil, types.CreateIfNotExist{})
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkLocalCreateThenInvokeActor(b *testing.B) {
	reg := localregistry.NewLocalRegistry()
	env, err := virtual.NewEnvironment(context.Background(), "serverID1", reg, nil, defaultOptsWASM)
	require.NoError(b, err)
	defer env.Close(context.Background())

	ctx := context.Background()

	_, err = reg.RegisterModule(ctx, "bench-ns", "test-module", utilWasmBytes, registry.ModuleOptions{})
	require.NoError(b, err)

	defer reportOpsPerSecond(b)()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		actorID := fmt.Sprintf("%d", i)
		_, err = env.InvokeActor(ctx, "bench-ns", actorID, "test-module", "incFast", nil, types.CreateIfNotExist{})
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkLocalActorToActorCommunication(b *testing.B) {
	reg := localregistry.NewLocalRegistry()
	env, err := virtual.NewEnvironment(context.Background(), "serverID1", reg, nil, defaultOptsWASM)
	require.NoError(b, err)
	defer env.Close(context.Background())

	ctx := context.Background()

	_, err = reg.RegisterModule(ctx, "bench-ns", "test-module", utilWasmBytes, registry.ModuleOptions{})
	require.NoError(b, err)

	invokeReq := types.InvokeActorRequest{
		ActorID:   "b",
		Operation: "incFast",
		Payload:   nil,
	}
	marshaled, err := json.Marshal(invokeReq)
	require.NoError(b, err)

	defer reportOpsPerSecond(b)()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = env.InvokeActor(ctx, "bench-ns", "a", "test-module", "invokeActor", marshaled, types.CreateIfNotExist{})
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
func TestBenchmarkFoundationDBRegistryInvokeActor(t *testing.T) {
	testSimpleBench(t, 1*time.Microsecond, 10, 15*time.Second, false)
}

// Can't use the micro-benchmarking framework because we need concurrency.
func TestBenchmarkFoundationDBRegistryInvokeWorker(t *testing.T) {
	testSimpleBench(t, 1*time.Microsecond, 10, 15*time.Second, true)
}

func testSimpleBench(
	t *testing.T,
	invokeEvery time.Duration,
	numActors int,
	benchDuration time.Duration,
	useWorker bool,
) {
	// Uncomment to run.
	t.Skip()

	reg, err := fdbregistry.NewFoundationDBRegistry("")
	require.NoError(t, err)
	require.NoError(t, reg.UnsafeWipeAll())

	env, err := virtual.NewEnvironment(context.Background(), "serverID1", reg, nil, defaultOptsWASM)
	require.NoError(t, err)
	defer env.Close(context.Background())

	_, err = reg.RegisterModule(context.Background(), "bench-ns", "test-module", utilWasmBytes, registry.ModuleOptions{})
	require.NoError(t, err)

	for i := 0; i < numActors; i++ {
		actorID := fmt.Sprintf("%d", i)
		if useWorker {
			_, err = env.InvokeWorker(
				context.Background(), "bench-ns", "test-module",
				"incFast", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
		} else {
			_, err = env.InvokeActor(
				context.Background(), "bench-ns", actorID, "test-module",
				"incFast", nil, types.CreateIfNotExist{})
			require.NoError(t, err)
		}
	}

	sketch, err := ddsketch.NewDefaultDDSketch(0.01)
	require.NoError(t, err)

	var (
		ctx, cc    = context.WithCancel(context.Background())
		outerWg    sync.WaitGroup
		innerWg    sync.WaitGroup
		benchState = &benchState{
			invokeLatency: sketch,
		}
	)

	outerWg.Add(1)
	go func() {
		defer outerWg.Done()

		for i := 0; ; i++ {
			i := 1 // Capture for async goroutine.
			select {
			default:
				innerWg.Add(1)
				go func() {
					defer innerWg.Done()

					start := time.Now()
					if !useWorker {
						actorID := fmt.Sprintf("%d", i%numActors)
						_, err = env.InvokeActor(
							ctx, "bench-ns", actorID, "test-module",
							"incFast", nil, types.CreateIfNotExist{})
						if err != nil {
							panic(err)
						}
					} else {
						_, err = env.InvokeWorker(
							ctx, "bench-ns", "test-module",
							"incFast", nil, types.CreateIfNotExist{})
						if err != nil {
							panic(err)
						}
					}

					benchState.track(time.Since(start))
				}()
				time.Sleep(invokeEvery)
			case <-ctx.Done():
				return
			}
		}
	}()

	time.Sleep(benchDuration)
	cc()
	outerWg.Wait()
	innerWg.Wait()

	fmt.Println("Inputs")
	fmt.Println("    invokeEvery", invokeEvery)
	fmt.Println("    numActors", numActors)
	fmt.Println("Results")
	fmt.Println("    numInvokes", benchState.getNumInvokes())
	fmt.Println("    invoke/s", float64(benchState.getNumInvokes())/benchDuration.Seconds())
	fmt.Println("    median latency", getQuantile(t, benchState.invokeLatency, 0.5))
	fmt.Println("    p95 latency", getQuantile(t, benchState.invokeLatency, 0.95), "ms")
	fmt.Println("    p99 latency", getQuantile(t, benchState.invokeLatency, 0.99), "ms")
	fmt.Println("    p99.9 latency", getQuantile(t, benchState.invokeLatency, 0.999), "ms")
	fmt.Println("    max latency", getQuantile(t, benchState.invokeLatency, 1.0), "ms")

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

func (b *benchState) getNumInvokes() int {
	b.RLock()
	defer b.RUnlock()

	return b.numInvokes
}

func (b *benchState) track(x time.Duration) {
	b.Lock()
	defer b.Unlock()

	b.invokeLatency.Add(float64(x.Milliseconds()))
	b.numInvokes++
}
