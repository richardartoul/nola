package fdbregistry

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/richardartoul/nola/virtual/registry"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/stretchr/testify/require"
)

func TestFDBRegistry(t *testing.T) {
	t.Skip("TODO: Only skip locally, but run in CI")

	registry.TestAllCommon(t, func() registry.Registry {
		registry, err := NewFoundationDBRegistry("")
		require.NoError(t, err)

		registry.UnsafeWipeAll()

		return registry
	})
}

func TestBenchFoundationDBKVGetVersionStamp(t *testing.T) {
	testBenchFoundationDBKVGetVersionStamp(t, 1*time.Microsecond, 15*time.Second)
}

func testBenchFoundationDBKVGetVersionStamp(
	t *testing.T,
	invokeEvery time.Duration,
	benchDuration time.Duration,
) {
	// Uncomment to run.
	t.Skip()

	reg, err := NewFoundationDBRegistry("")
	require.NoError(t, err)
	require.NoError(t, reg.UnsafeWipeAll())

	sketch, err := ddsketch.NewDefaultDDSketch(0.01)
	require.NoError(t, err)

	var (
		ctx, cc    = context.WithCancel(context.Background())
		wg         sync.WaitGroup
		benchState = &benchState{
			callLatency: sketch,
		}
		ticker = time.NewTicker(invokeEvery)
	)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 1; ; i++ {
			i := i // Capture for async goroutine.
			select {
			case <-ticker.C:
				wg.Add(1)
				go func() {
					defer wg.Done()

					start := time.Now()
					_, err := reg.GetVersionStamp(ctx)
					if err != nil {
						panic(err)
					}

					benchState.trackCallLatency(time.Since(start))
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
	ticker.Stop()
	cc()
	wg.Wait()

	fmt.Println("Inputs")
	fmt.Println("    invokeEvery", invokeEvery)
	fmt.Println("Results")
	fmt.Println("    numInvokes", benchState.getNumCalls())
	fmt.Println("    invoke/s", float64(benchState.getNumCalls())/benchDuration.Seconds())
	fmt.Println("    median latency", getQuantile(t, benchState.callLatency, 0.5))
	fmt.Println("    p95 latency", getQuantile(t, benchState.callLatency, 0.95), "ms")
	fmt.Println("    p99 latency", getQuantile(t, benchState.callLatency, 0.99), "ms")
	fmt.Println("    p99.9 latency", getQuantile(t, benchState.callLatency, 0.999), "ms")

	t.Fail() // Fail so it prints output.
}

type benchState struct {
	sync.RWMutex

	numCalls    int
	callLatency *ddsketch.DDSketch
}

func (b *benchState) setNumInvokes(x int) {
	b.Lock()
	defer b.Unlock()

	b.numCalls = x
}

func (b *benchState) getNumCalls() int {
	b.RLock()
	defer b.RUnlock()

	return b.numCalls
}

func (b *benchState) trackCallLatency(x time.Duration) {
	b.Lock()
	defer b.Unlock()

	b.callLatency.Add(float64(x.Milliseconds()))
}

func getQuantile(t *testing.T, sketch *ddsketch.DDSketch, q float64) float64 {
	quantile, err := sketch.GetValueAtQuantile(q)
	require.NoError(t, err)
	return quantile
}
