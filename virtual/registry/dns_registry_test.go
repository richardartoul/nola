package registry

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDNSRegistry(t *testing.T) {
	reg, err := NewDNSRegistry(&fakeResolver{})
	require.NoError(t, err)

}

type fakeResolver struct {
}

func (f *fakeResolver) LookupIP(host string) ([]net.IP, error) {
	return []net.IP{
		net.ParseIP("127.0.0.1"),
		net.ParseIP("127.0.0.2"),
		net.ParseIP("127.0.0.3"),
	}, nil
}
