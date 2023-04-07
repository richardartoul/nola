package filecache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/richardartoul/nola/virtual"
	"github.com/richardartoul/nola/virtual/registry/localregistry"
	"github.com/richardartoul/nola/virtual/types"
	"golang.org/x/exp/slog"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/stretchr/testify/require"
)

// On OSX may need to run these commands before benchmarking:
//
// sudo ulimit -n 6049
// sudo sysctl -w kern.ipc.somaxconn=30000
func TestFileCacheBenchmark(t *testing.T) {
	t.Skip()
	var (
		port          = 9090
		benchDuration = 15 * time.Second
		invokeEvery   = 250 * time.Microsecond
		chunkSize     = 1 << 20
		fetchSize     = 1 << 24
		fileSize      = 1 << 28
		getRangeSize  = 1 << 20
		fetcher       = newTestFetcher(fileSize, false)
		cache         = newTestCache()
	)
	registry := localregistry.NewLocalRegistry()
	env, err := virtual.NewEnvironment(
		context.Background(),
		slog.Default(),
		"test-server-id", registry,
		virtual.NewHTTPClient(), virtual.EnvironmentOptions{
			Discovery: virtual.DiscoveryOptions{
				DiscoveryType: virtual.DiscoveryTypeLocalHost,
				Port:          port,
			},
			// Make sure the benchmark tests the RPC/HTTP stack, not just the
			// in-memory virtual.Environment code.
			ForceRemoteProcedureCalls: true,
		})
	if err != nil {
		slog.Error("error creating virtual environment", slog.Any("error", err))
		return
	}
	err = env.RegisterGoModule(
		types.NewNamespacedIDNoType("bench-ns", "file-cache"),
		NewFileCacheModule(chunkSize, fetchSize, fetcher, cache))
	require.NoError(t, err)

	server := virtual.NewServer(registry, env)
	go func() {
		if err := server.Start(port); err != nil {
			panic(err)
		}
	}()
	// Wait for server to start.
	time.Sleep(time.Second)

	instantiatePayload, err := json.Marshal(&FileCacheInstantiatePayload{
		FileSize: fileSize,
	})
	require.NoError(t, err)

	var (
		innerWg    sync.WaitGroup
		benchState = newBenchState(t)
		ctx, cc    = context.WithCancel(context.Background())
	)
	go func() {
		for i := 0; ; i++ {
			select {
			default:
				innerWg.Add(1)
				go func() {
					defer innerWg.Done()

					req := GetRangeRequest{
						StartOffset: rand.Intn(fileSize),
					}
					if req.StartOffset >= fileSize-1 {
						req.StartOffset = fileSize - 10
					}
					req.EndOffset = req.StartOffset + getRangeSize
					if req.EndOffset >= fileSize {
						req.EndOffset = fileSize - 1
					}

					marshaled, err := json.Marshal(&req)
					if err != nil {
						panic(err)
					}

					start := time.Now()
					resultStream, err := env.InvokeActorStream(
						context.Background(), "bench-ns", "bench-actor", "file-cache",
						"getRange", marshaled, types.CreateIfNotExist{
							InstantiatePayload: instantiatePayload,
						})
					if err != nil {
						panic(err)
					}
					defer resultStream.Close()

					io.Copy(io.Discard, resultStream)

					benchState.track(time.Since(start), req.EndOffset-req.StartOffset)
				}()
				time.Sleep(invokeEvery)
			case <-ctx.Done():
				return
			}
		}
	}()

	time.Sleep(benchDuration)
	cc()
	start := time.Now()
	innerWg.Wait()
	if time.Since(start) > time.Second {
		panic("waiting for all goroutines to shutdown took too long, service overloaded")
	}

	fmt.Println("Inputs")
	fmt.Println("    invokeEvery", invokeEvery)
	fmt.Println("    chunkSize", chunkSize)
	fmt.Println("    fetchSize", fetchSize)
	fmt.Println("    fileSize", fileSize)
	fmt.Println("    getRangeSize", getRangeSize)
	fmt.Println("    invokeEvery", invokeEvery)
	fmt.Println("Results")
	fmt.Println("    numInvokes", benchState.numInvokes)
	fmt.Println("    invoke/s", float64(benchState.numInvokes)/benchDuration.Seconds())
	fmt.Println("    numMiBs", benchState.numBytes/1000/1000)
	fmt.Println("    MiB/s", float64(benchState.numBytes/1000/1000)/benchDuration.Seconds())
	fmt.Println("    median latency", getQuantile(t, benchState.invokeLatency, 0.5))
	fmt.Println("    p95 latency", getQuantile(t, benchState.invokeLatency, 0.95), "ms")
	fmt.Println("    p99 latency", getQuantile(t, benchState.invokeLatency, 0.99), "ms")
	fmt.Println("    p99.9 latency", getQuantile(t, benchState.invokeLatency, 0.999), "ms")
	fmt.Println("    max latency", getQuantile(t, benchState.invokeLatency, 1.0), "ms")

	t.Fail()
}

func getQuantile(t *testing.T, sketch *ddsketch.DDSketch, q float64) float64 {
	quantile, err := sketch.GetValueAtQuantile(q)
	require.NoError(t, err)
	return quantile
}

type benchState struct {
	sync.RWMutex

	invokeLatency *ddsketch.DDSketch
	numInvokes    int
	numBytes      int
}

func newBenchState(t *testing.T) *benchState {
	sketch, err := ddsketch.NewDefaultDDSketch(0.01)
	require.NoError(t, err)

	return &benchState{
		invokeLatency: sketch,
	}
}

func (b *benchState) track(latency time.Duration, numBytes int) {
	b.Lock()
	defer b.Unlock()

	b.invokeLatency.Add(float64(latency.Milliseconds()))
	b.numInvokes++
	b.numBytes += numBytes
}
