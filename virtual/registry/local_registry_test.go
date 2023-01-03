package registry

import (
	"testing"
)

func TestLocalRegistry(t *testing.T) {
	testAllCommon(t, func() Registry { return NewLocalRegistry() })
}
