package durablewazero

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wapc/wapc-go/engines/wazero"
)

var utilWasmBytes []byte

func init() {
	fBytes, err := ioutil.ReadFile("../../testdata/tinygo/util/main.wasm")
	if err != nil {
		panic(err)
	}
	utilWasmBytes = fBytes
}

func TestDurable(t *testing.T) {
	ctx := context.Background()

	module, err := NewModule(ctx, wazero.Engine(), testHost, utilWasmBytes)
	require.NoError(t, err)
	defer func() {
		panicIfErr(module.Close(ctx))
	}()

	for i := 0; i < 2; i++ {
		object, err := module.Instantiate(ctx, "a")
		require.NoError(t, err)

		// This function is not defined so this should error out.
		_, err = object.Invoke(ctx, "does-not-exist", []byte("123"))
		require.Error(t, err)

		result, err := object.Invoke(ctx, "inc", nil)
		require.NoError(t, err)

		// Each iteration returns 1 because we're closing at the end.
		require.Equal(t, int64(1), getCount(t, result))
		require.NoError(t, object.Close(ctx))
	}

	object, err := module.Instantiate(ctx, "a")
	require.NoError(t, err)

	// Create some state.
	_, err = object.Invoke(ctx, "inc", []byte("123"))
	require.NoError(t, err)

	// Snapshot the existing state, then close the object. This will evict
	// the instance entirely and release all its memory.
	serBuf := bytes.NewBuffer(nil)
	require.NoError(t, object.Snapshot(ctx, serBuf))
	require.NoError(t, object.Close(ctx))

	// Since the Object was closed previously, this will create a new WASM
	// instance.
	object, err = module.Instantiate(ctx, "a")
	require.NoError(t, err)
	defer object.Close(ctx)

	// Hydrate it from the state we snapshotted previously.
	require.NoError(t, object.Hydrate(ctx, serBuf, serBuf.Len()))

	// If Snapshot()/Hydrate() works then the next inc should take us to
	// 2 instead of 1.
	result, err := object.Invoke(ctx, "inc", nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), getCount(t, result))
}

func testHost(ctx context.Context, binding, namespace, operation string, payload []byte) ([]byte, error) {
	return nil, fmt.Errorf(
		"testHotNotImplemented [%s::%s::%s::%s)",
		binding, namespace, operation, string(payload))
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func getCount(t *testing.T, v []byte) int64 {
	x, err := strconv.Atoi(string(v))
	require.NoError(t, err)
	return int64(x)
}
