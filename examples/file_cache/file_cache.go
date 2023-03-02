package blobcache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/richardartoul/nola/virtual"
	"github.com/richardartoul/nola/virtual/registry"
)

type Fetcher interface {
	FetchRange(ctx context.Context, offset, length int) (io.ReadCloser, error)
}

type ChunkCache interface {
	Get(b []byte, chunkIdx int) ([]byte, bool, error)
	Put(chunkIdx int, b []byte) error
}

type FileCacheActor struct {
	sync.Mutex

	// Constants.
	fileSize  int
	chunkSize int
	fetchSize int

	// Dependencies.
	fetcher    Fetcher
	chunkCache ChunkCache
}

func NewFileCacheActor(
	fileSize int,
	chunkSize int,
	fetchSize int,

	fetcher Fetcher,
	chunkCache ChunkCache,
) (virtual.ActorStream, error) {
	if fetchSize%chunkSize != 0 || chunkSize > fetchSize {
		return nil, fmt.Errorf("%d does not cleanly divide %d", fetchSize, chunkSize)
	}

	return &FileCacheActor{
		fileSize:  fileSize,
		chunkSize: chunkSize,
		fetchSize: fetchSize,

		fetcher:    fetcher,
		chunkCache: chunkCache,
	}, nil
}

type GetRangeRequest struct {
	StartOffset int `json:"start_offset"`
	EndOffset   int `json:"end_offset"`
}

func (f *FileCacheActor) InvokeStream(
	ctx context.Context,
	operation string,
	payload []byte,
	transaction registry.ActorKVTransaction,
) (io.ReadCloser, error) {
	switch operation {
	case "getRange":
		req := &GetRangeRequest{}
		if err := json.Unmarshal(payload, req); err != nil {
			return nil, fmt.Errorf("error unmarshaling GetRangeRequest: %w", err)
		}

		if req.StartOffset < 0 ||
			req.EndOffset < 0 ||
			req.EndOffset <= req.StartOffset ||
			req.EndOffset > f.fileSize {
			return nil, fmt.Errorf("invalid GetRangeRequest: %+v, f.fileSize: %d", req, f.fileSize)
		}

		reader, writer := io.Pipe()
		go func() {
			f.getRange(ctx, writer, req.StartOffset, req.EndOffset)
		}()
		return reader, nil
	default:
		return nil, fmt.Errorf("unhandled operation: %s", operation)
	}
}

func (f *FileCacheActor) Close(ctx context.Context) error {
	return nil
}

func (f *FileCacheActor) getRange(
	ctx context.Context,
	w *io.PipeWriter,
	start,
	end int,
) {
	chunksToRead := f.rangeToChunkIndexes(start, end)
	for _, chunk := range chunksToRead {
		if err := f.copyChunk(ctx, w, chunk); err != nil {
			w.CloseWithError(err)
			return
		}
	}
	w.Close()
}

func (f *FileCacheActor) copyChunk(
	ctx context.Context,
	w io.Writer,
	toRead chunkToRead,
) error {
	// First try and copy the requested chunk out from the cache.
	// TODO: pool []byte.
	chunk, ok, err := f.chunkCache.Get(nil, toRead.idx)
	if err != nil {
		return fmt.Errorf("error copying chunk from cache: %w", err)
	}
	if ok {
		// Chunk was in the cache so it was copied, we're done.
		_, err := w.Write(chunk[toRead.start:toRead.end])
		if err != nil {
			return fmt.Errorf("error writing chunk to output writer: %w", err)
		}
		return nil
	}

	// Chunk was not in cache, we need to fetch.
	// TODO: deduplicate fetches.
	start, end := f.chunkIndexToFetchRange(toRead.idx)
	r, err := f.fetcher.FetchRange(ctx, start, end)
	if err != nil {
		return fmt.Errorf(
			"error fetching range: [%d:%d] for chunk idx: %d",
			start, end, toRead.idx)
	}

	// TODO: Pool?
	var (
		buf       = make([]byte, 0, f.chunkSize)
		remaining = end - start
		chunkIdx  = f.offsetToChunkIndex(start)
	)
	for i := 0; remaining > 0; i++ {
		toCopy := remaining
		if toCopy > f.chunkSize {
			toCopy = f.chunkSize
		}
		// TODO: Don't allocate.
		buf := bytes.NewBuffer(buf[:0])
		// TODO: Reusable buf.
		n, err := io.CopyN(buf, r, int64(toCopy))
		if err != nil {
			return fmt.Errorf("error copying from fetch: %w", err)
		}
		if n != int64(toCopy) {
			return fmt.Errorf(
				"expected to copy: %d bytes but copied: %d",
				toCopy, n)
		}

		err = f.chunkCache.Put(chunkIdx, buf.Bytes())
		if err != nil {
			return fmt.Errorf("error storing chunk: %d in cache: %w", chunkIdx, err)
		}
		remaining -= toCopy
		chunkIdx++
	}

	// TODO: pool []byte.
	chunk, ok, err = f.chunkCache.Get(nil, toRead.idx)
	if err != nil {
		return fmt.Errorf("error copying chunk from cache: %w", err)
	}
	if ok {
		// Chunk was in the cache so it was copied, we're done.
		_, err := w.Write(chunk[toRead.start:toRead.end])
		if err != nil {
			return fmt.Errorf("error writing chunk to output writer: %w", err)
		}
		return nil
	}

	return fmt.Errorf("chunk: %d was not in cache after fetch", toRead.idx)
}

type chunkToRead struct {
	idx   int
	start int
	end   int
}

// TODO: Refactor/clean this function.
func (f *FileCacheActor) rangeToChunkIndexes(start, end int) []chunkToRead {
	var chunkIndexes []chunkToRead

	remaining := end - start
	curr := start
	for i := 0; remaining > 0; i++ {
		if i == 0 {
			chunkIdx := curr / f.chunkSize
			chunkStartOffset := curr % f.chunkSize
			chunkEndOffset := f.chunkSize
			if remaining < f.chunkSize {
				chunkEndOffset = chunkStartOffset + remaining
			}
			chunkIndexes = append(chunkIndexes, chunkToRead{
				idx:   chunkIdx,
				start: chunkStartOffset,
				end:   chunkEndOffset,
			})
			remaining -= chunkEndOffset - chunkStartOffset
			curr += f.chunkSize
			continue
		}

		chunkIndexes = append(chunkIndexes, chunkToRead{
			idx:   i / f.chunkSize,
			start: 0,
			end:   f.chunkSize,
		})
		remaining -= f.chunkSize
		curr += f.chunkSize
	}
	last := chunkIndexes[len(chunkIndexes)-1]
	if last.end*last.idx > end {
		last.end = end - last.idx*f.chunkSize
	}

	return chunkIndexes
}

func (f *FileCacheActor) offsetToChunkIndex(offset int) int {
	return offset / f.chunkSize
}

func (f *FileCacheActor) chunkIndexToFetchRange(idx int) (int, int) {
	var (
		start = ((idx * f.chunkSize) / f.fetchSize) * f.fetchSize
		end   = start + f.fetchSize
	)
	if end > f.fileSize {
		end = f.fileSize
	}
	return start, end
}
