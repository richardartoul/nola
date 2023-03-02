package filecache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFileCacheActorConcurrency(t *testing.T) {
	var (
		fileSize  = 1 << 24
		chunkSize = 1 << 16
		fetchSize = 1 << 20

		fetcher = newTestFetcher(fileSize)
		cache   = newTestCache()
	)

	fileCache, err := NewFileCacheActor(fileSize, chunkSize, fetchSize, fetcher, cache)
	require.NoError(t, err)

	var (
		numWorkers      = 16
		numOpsPerWorker = 10_000
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
				req.EndOffset = req.StartOffset + 1
				// req.EndOffset = req.StartOffset + rand.Intn(fileSize-req.StartOffset)
				// if req.EndOffset == req.StartOffset {
				// 	req.EndOffset = req.StartOffset + 1
				// }

				marshaled, err := json.Marshal(&req)
				if err != nil {
					panic(err)
				}
				reader, err := fileCache.InvokeStream(context.Background(), "getRange", marshaled, nil)
				if err != nil {
					panic(err)
				}
				result, err := ioutil.ReadAll(reader)
				if err != nil {
					panic(err)
				}
				require.Equal(
					t,
					string(fetcher.(*testFetcher).file[req.StartOffset:req.EndOffset]),
					string(result), fmt.Sprintf("%d->%d", req.StartOffset, req.EndOffset))
				fmt.Println("matched!!!")
			}
		}()
	}

	wg.Wait()
}

type testFetcher struct {
	file []byte
}

func newTestFetcher(size int) Fetcher {
	var (
		runes = "abcdefghijklmnopqrstuvwxyz"
		file  = make([]byte, size)
	)
	for i := 0; i < size; i++ {
		file[i] = runes[rand.Intn(len(runes))]
	}

	return &testFetcher{
		file: file,
	}
}

func (t *testFetcher) FetchRange(ctx context.Context, start, end int) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewBuffer(t.file[start:end])), nil
}

type testCache struct {
	sync.Mutex

	m map[int][]byte
}

func newTestCache() ChunkCache {
	return &testCache{
		m: make(map[int][]byte),
	}
}

func (t *testCache) Get(b []byte, chunkIdx int) ([]byte, bool, error) {
	t.Lock()
	defer t.Unlock()

	existing, ok := t.m[chunkIdx]
	if !ok {
		return nil, false, nil
	}

	if len(existing) == 0 {
		panic("should not be empty")
	}

	b = append(b, existing...)
	return b, true, nil
}

func (t *testCache) Put(chunkIdx int, v []byte) error {
	t.Lock()
	defer t.Unlock()

	if len(v) == 0 {
		panic("should not be empty")
	}
	t.m[chunkIdx] = append([]byte(nil), v...)
	return nil
}
