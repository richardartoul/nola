//go:build !race

package filecache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFileCacheActorConcurrencyNoFaults(t *testing.T) {
	testFileCacheActorConcurrency(t, false)
}

func TestFileCacheActorConcurrencyWithFaults(t *testing.T) {
	testFileCacheActorConcurrency(t, true)
}

func testFileCacheActorConcurrency(t *testing.T, injectFaults bool) {
	var (
		fileSize     = 1 << 24
		maxRangeSize = 1 << 10
		chunkSize    = 1 << 16
		fetchSize    = 1 << 20

		fetcher = newTestFetcher(fileSize, injectFaults)
		cache   = newTestCache()
	)

	fileCache, err := NewFileCacheActor(fileSize, chunkSize, fetchSize, fetcher, cache)
	require.NoError(t, err)

	var (
		numWorkers      = runtime.NumCPU()
		numOpsPerWorker = 100_000
		wg              sync.WaitGroup
	)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var (
				rand = rand.New(rand.NewSource(time.Now().UnixNano()))
			)
			for j := 0; j < numOpsPerWorker; j++ {
				req := GetRangeRequest{
					StartOffset: rand.Intn(fileSize),
				}
				if req.StartOffset >= fileSize-1 {
					req.StartOffset = fileSize - 10
				}
				req.EndOffset = req.StartOffset + rand.Intn(maxRangeSize) + 1
				if req.EndOffset >= fileSize {
					req.EndOffset = fileSize - 1
				}

				marshaled, err := json.Marshal(&req)
				if err != nil {
					panic(err)
				}
				reader, err := fileCache.InvokeStream(context.Background(), "getRange", marshaled)
				if err != nil {
					panic(err)
				}
				result, err := ioutil.ReadAll(reader)
				if err != nil {
					if injectFaults {
						if strings.Contains(err.Error(), "not in cache after fetch") {
							// Ignore this error. It happens because we're constantly deleting random
							// items from the cache as a stress test.
							continue
						}
						if strings.Contains(err.Error(), "fake error") {
							// Ignore this error. It happens because make the testFetcher randomly
							// return errors as an additional stress test.
							continue
						}
					}

					panic(err)
				}

				require.Equal(
					t,
					string(fetcher.(*testFetcher).file[req.StartOffset:req.EndOffset]),
					string(result), fmt.Sprintf("%d->%d", req.StartOffset, req.EndOffset))

				if j%100 == 0 && injectFaults {
					cache.deleteRandom()
				}
			}
		}()
	}

	wg.Wait()
}

type testFetcher struct {
	file         []byte
	injectFaults bool
}

func newTestFetcher(size int, injectFaults bool) Fetcher {
	var (
		runes = "abcdefghijklmnopqrstuvwxyz"
		file  = make([]byte, size)
		rand  = rand.New(rand.NewSource(time.Now().UnixNano()))
	)
	for i := 0; i < size; i++ {
		file[i] = runes[rand.Intn(len(runes))]
	}

	return &testFetcher{
		file:         file,
		injectFaults: injectFaults,
	}
}

func (t *testFetcher) FetchRange(ctx context.Context, start, end int) (io.ReadCloser, error) {
	if t.injectFaults && rand.Intn(100) == 0 {
		return nil, errors.New("fake error")
	}

	return io.NopCloser(bytes.NewBuffer(t.file[start:end])), nil
}

type testCache struct {
	sync.Mutex

	m      map[int][]byte
	maxIdx int
}

func newTestCache() *testCache {
	return &testCache{
		m: make(map[int][]byte),
	}
}

func (t *testCache) Get(b []byte, chunkIdx int) ([]byte, bool, error) {
	t.Lock()
	existing, ok := t.m[chunkIdx]
	t.Unlock()
	if !ok {
		return nil, false, nil
	}

	if len(existing) == 0 {
		panic("should not be empty")
	}

	b = append(b[:0], existing...)
	return b, true, nil
}

func (t *testCache) Put(chunkIdx int, v []byte) error {
	if len(v) == 0 {
		panic("should not be empty")
	}

	clone := append([]byte(nil), v...)

	t.Lock()
	t.m[chunkIdx] = clone
	if chunkIdx > t.maxIdx {
		t.maxIdx = chunkIdx
	}
	t.Unlock()

	return nil
}

func (t *testCache) deleteRandom() {
	t.Lock()
	defer t.Unlock()

	idx := rand.Intn(t.maxIdx)
	delete(t.m, idx)
}
