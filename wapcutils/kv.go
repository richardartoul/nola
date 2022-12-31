package wapcutils

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ExtractKVFromPutPayload extracts the encoded KV pair from an encoded []byte payload.
func ExtractKVFromPutPayload(payload []byte) ([]byte, []byte, error) {
	keyLen, n := binary.Varint(payload)
	if n == 0 {
		return nil, nil, errors.New("malformed PUT payload, unable to parse key varint")
	}
	if keyLen > int64(len(payload)) {
		return nil, nil, fmt.Errorf("keyLen: %d > len(payload): %d", keyLen, len(payload))
	}

	var (
		keyStart = int64(n)
		keyEnd   = keyStart + keyLen
	)
	if keyEnd < keyStart || keyEnd < 0 || keyEnd > int64(len(payload)) {
		return nil, nil, fmt.Errorf(
			"illegal keyEnd: %d, keyStart: %d, payload: %v",
			keyEnd, keyStart, string(payload))
	}
	return payload[keyStart:keyEnd], payload[keyEnd:], nil
}

// EncodePUTPayload encodes the provided KV pair into dst (returning a possible re-allocated
// byte slice) such that it can be decoded by ExtractKVFromPutPayload.
func EncodePutPayload(dst []byte, key, value []byte) []byte {
	if cap(dst) <= binary.MaxVarintLen64 {
		dst = make([]byte, 0, binary.MaxVarintLen64)
	}
	dst = dst[:binary.MaxVarintLen64]
	dst = dst[:binary.PutVarint(dst, int64(len(key)))]
	dst = append(dst, key...)
	dst = append(dst, value...)
	return dst
}
