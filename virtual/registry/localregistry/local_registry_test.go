package localregistry

import (
	"testing"

	"github.com/richardartoul/nola/virtual/registry"
)

func TestLocalRegistry(t *testing.T) {
	registry.TestAllCommon(t, func() registry.Registry {
		return NewLocalRegistry()
	})
}
