package registry

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/stretchr/testify/require"
)

// TODO NEXT: This is too slow. Need to cache it.

func TestBenchFoundationDBKVGetVersionStamp(t *testing.T) {
	testBenchFoundationDBKVGetVersionStamp(t, 10*time.Microsecond, 15*time.Second)
}

func testBenchFoundationDBKVGetVersionStamp(
	t *testing.T,
	invokeEvery time.Duration,
	benchDuration time.Duration,
) {
	// Uncomment to run.
	// t.Skip()

	kv, err := newFDBKV("")
	require.NoError(t, err)

	sketch, err := ddsketch.NewDefaultDDSketch(0.01)
	require.NoError(t, err)

	var (
		ctx, cc    = context.WithCancel(context.Background())
		wg         sync.WaitGroup
		benchState = &benchState{
			invokeLatency: sketch,
		}
		ticker = time.NewTicker(invokeEvery)
	)
	go func() {
		for i := 1; ; i++ {
			i := i // Capture for async goroutine.
			select {
			case <-ticker.C:
				wg.Add(1)
				go func() {
					defer wg.Done()

					start := time.Now()
					_, err := kv.transact(func(tr transaction) (any, error) {
						_, err := tr.getVersionStamp()
						return nil, err
					})
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
	ticker.Stop()
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

// TODO: Fix field/method names.
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

func getQuantile(t *testing.T, sketch *ddsketch.DDSketch, q float64) float64 {
	quantile, err := sketch.GetValueAtQuantile(q)
	require.NoError(t, err)
	return quantile
}
