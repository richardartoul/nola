package durablewazero

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/wapc/wapc-go"
	"github.com/wapc/wapc-go/engines/wazero"
)

const (
	wasmPageSize = 1 << 16
)

type object struct {
	sync.Mutex
	instance wapc.Instance
	onClose  func()
}

func newObject(
	instance wapc.Instance,
	onClose func(),
) *object {
	return &object{
		instance: instance,
		onClose:  onClose,
	}
}

func (o *object) MemoryUsageBytes() int {
	return int(o.instance.MemorySize())
}

func (o *object) Invoke(
	ctx context.Context,
	operation string,
	payload []byte,
) ([]byte, error) {
	o.Lock()
	defer o.Unlock()

	// TODO: Make byte ownership more clear?
	return o.instance.Invoke(ctx, operation, payload)
}

// TODO: Make this resilient to double-close.
// TODO: Other methods like Snapshot/Invoke etc should return error after close.
func (o *object) Close(ctx context.Context) error {
	o.Lock()
	defer o.Unlock()

	o.onClose()
	return o.instance.Close(ctx)
}

func (o *object) Snapshot(
	ctx context.Context,
	w io.Writer,
) error {
	o.Lock()
	defer o.Unlock()

	memory := o.instance.(*wazero.Instance).UnwrapModule().Memory()
	bytes, ok := memory.Read(0, memory.Size())
	if !ok {
		return fmt.Errorf(
			"error snapshotting object: memory.Read() return false for range: %d->%d",
			0, memory.Size())
	}
	_, err := w.Write(bytes)
	if err != nil {
		return fmt.Errorf(
			"error snapshotting object: write failed with error: %w", err)
	}
	return nil
}

// This method is wrong but its close enough for benchmarking.
func (o *object) SnapshotIncremental(
	ctx context.Context,
	prev []byte,
	w io.Writer,
) error {
	panic("implemented incorrectly only for benchmarking.")
	o.Lock()
	defer o.Unlock()

	memory := o.instance.(*wazero.Instance).UnwrapModule().Memory()
	memBytes, ok := memory.Read(0, memory.Size())
	if !ok {
		return fmt.Errorf(
			"error snapshotting object: memory.Read() return false for range: %d->%d",
			0, memory.Size())
	}

	for i := 0; i < len(memBytes); i += 128 {
		if i+128 >= len(prev) || !bytes.Equal(memBytes[i:i+128], prev[i:i+128]) {
			if _, err := w.Write(memBytes[i : i+128]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *object) Hydrate(
	ctx context.Context,
	r io.Reader,
	readerSize int,
) error {
	o.Lock()
	defer o.Unlock()

	var (
		memory  = o.instance.(*wazero.Instance).UnwrapModule().Memory()
		memSize = int(memory.Size())
	)
	if readerSize > memSize {
		var (
			additionalBytesNeeded = readerSize - memSize
			additionalPagesNeeded = additionalBytesNeeded / wasmPageSize
		)
		if additionalBytesNeeded%wasmPageSize > 0 {
			additionalPagesNeeded++
		}
		memory.Grow(uint32(additionalPagesNeeded))
	}

	// Very important we do this *after* calling memory.Grow, otherwise the
	// memBytes slice may be "detached" from the underlying instance memory
	// and our writes won't "write through".
	memBytes, ok := memory.Read(0, memory.Size())
	if !ok {
		return fmt.Errorf(
			"error hydrating object: memory.Read() return false for range: %d->%d",
			0, memory.Size())
	}
	// Zero out existing memory just in case.
	for i := range memBytes {
		memBytes[i] = 0
	}

	_, err := io.CopyN(bytes.NewBuffer(memBytes[:0]), r, int64(readerSize))
	if err != nil {
		return fmt.Errorf("error hydrating object: error copying bytes from reader to memory: %w", err)
	}
	return nil
}
