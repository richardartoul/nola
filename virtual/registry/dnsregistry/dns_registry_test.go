package dnsregistry

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/stretchr/testify/require"
)

// TestDNSRegistrySimple is a simple test of the DNS registry. It tests that
// the background discovery loop works and that EnsureActivation uses
// consistent hashing to pick a server.
func TestDNSRegistrySimple(t *testing.T) {
	resolver := &fakeResolver{}
	reg, err := NewDNSRegistry(resolver, "test", 9090, DNSRegistryOptions{
		ResolveEvery: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	defer func() {
		if err := reg.Close(context.Background()); err != nil {
			panic(err)
		}
	}()

	_, err = reg.EnsureActivation(context.Background(), "ns1", "a", "test-module")
	require.True(t, strings.Contains(err.Error(), "hashring is empty"))

	// Should be a no-op.
	_, err = reg.Heartbeat(context.Background(), "serverID", registry.HeartbeatState{})
	require.NoError(t, err)

	// Should always return a constant.
	for i := 0; i < 100; i++ {
		versionStamp, err := reg.GetVersionStamp(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(1), versionStamp)
	}

	resolver.setIPs([]net.IP{
		net.ParseIP("127.0.0.1"),
		net.ParseIP("127.0.0.2"),
		net.ParseIP("127.0.0.3"),
	})

	for {
		activations, err := reg.EnsureActivation(context.Background(), "ns1", "a", "test-module")
		if err != nil {
			time.Sleep(time.Millisecond)
			continue
		}

		require.Equal(t, 1, len(activations))
		require.Equal(t, "a", activations[0].ActorID().ID)
		require.Equal(t, "test-module", activations[0].ModuleID().ID)
		require.Equal(t, "127.0.0.3:9090", activations[0].Address())
		require.Equal(t, DNSServerID, activations[0].ServerID())
		require.Equal(t, DNSServerVersion, activations[0].ServerVersion())
		break
	}

}

type fakeResolver struct {
	sync.Mutex
	ips []net.IP
}

func (f *fakeResolver) setIPs(ips []net.IP) {
	f.Lock()
	defer f.Unlock()
	f.ips = ips
}

func (f *fakeResolver) LookupIP(host string) ([]net.IP, error) {
	if host != "test" {
		panic(fmt.Sprintf("wrong host: %s", host))
	}

	f.Lock()
	defer f.Unlock()
	return f.ips, nil
}
