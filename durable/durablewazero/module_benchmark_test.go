package durablewazero

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wapc/wapc-go/engines/wazero"
)

func BenchmarkObjectExecution(b *testing.B) {
	ctx := context.Background()

	module, err := NewModule(ctx, wazero.Engine(), testHost, utilWasmBytes)
	require.NoError(b, err)
	defer panicIfErr(module.Close(ctx))

	object, err := module.Instantiate(ctx, "a")
	require.NoError(b, err)

	buf := bytes.NewBuffer(nil)

	b.Run("no durability", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := object.Invoke(ctx, "inc", nil)
			if err != nil {
				panic(err)
			}
		}
		b.ReportMetric(0, "bytes/op")
	})

	b.Run("full snapshot", func(b *testing.B) {
		bytesWritten := 0
		for i := 0; i < b.N; i++ {
			_, err := object.Invoke(ctx, "inc", nil)
			if err != nil {
				panic(err)
			}

			buf.Reset()
			if err := object.Snapshot(ctx, buf); err != nil {
				panic(err)
			}
			bytesWritten += buf.Len()
		}
		b.ReportMetric(float64(bytesWritten)/float64(b.N), "bytes/op")
	})

	prevBuf := bytes.NewBuffer(nil)
	b.Run("incremental", func(b *testing.B) {
		bytesWritten := 0
		for i := 0; i < b.N; i++ {
			_, err := object.Invoke(ctx, "inc", nil)
			if err != nil {
				panic(err)
			}

			buf.Reset()
			if err := object.SnapshotIncremental(ctx, prevBuf.Bytes(), buf); err != nil {
				panic(err)
			}
			bytesWritten += buf.Len()
			prevBuf, buf = buf, prevBuf
		}
		b.ReportMetric(float64(bytesWritten)/float64(b.N), "bytes/op")
	})
}
