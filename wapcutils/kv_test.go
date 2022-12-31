package wapcutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoundtrip(t *testing.T) {
	k, v := []byte("key1"), []byte("val1")
	encoded := EncodePutPayload(nil, k, v)
	eK, eV, err := ExtractKVFromPutPayload(encoded)
	require.NoError(t, err)
	require.Equal(t, k, eK)
	require.Equal(t, v, eV)
}
