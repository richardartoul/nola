package durablewazero

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/wapc/wapc-go/engines/wazero"
	"testing"
)

func TestDoubleClose(t *testing.T) {
	ctx := context.Background()

	module, err := NewModule(ctx, wazero.Engine(), testHost, utilWasmBytes)
	require.NoError(t, err)

	object, err := module.Instantiate(ctx, "a")
	require.NoError(t, err)

	err = object.Close(ctx)
	require.NoError(t, err)

	err = object.Close(ctx)
	require.Nil(t, err)

	resp, err := object.Invoke(ctx, "", nil)
	require.Nil(t, resp)
	require.Error(t, err)

	err = object.Snapshot(ctx, nil)
	require.Error(t, err)
}
