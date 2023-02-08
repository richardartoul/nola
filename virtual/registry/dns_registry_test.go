package registry

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDNSRegistry(t *testing.T) {
	resolver := &fakeResolver{}
	reg, err := NewDNSRegistry(resolver, "test", DNSRegistryOptions{
		ResolveEvery: 100 * time.Millisecond,
	})
	require.NoError(t, err)

	_, err = reg.EnsureActivation(context.Background(), "ns1", "a")
	require.True(t, strings.Contains(err.Error(), "hashring is empty"))

	resolver.setIPs([]net.IP{
		net.ParseIP("127.0.0.1"),
		net.ParseIP("127.0.0.2"),
		net.ParseIP("127.0.0.3"),
	})

	for {
		activations, err := reg.EnsureActivation(context.Background(), "ns1", "a")
		if err != nil {
			time.Sleep(time.Millisecond)
			continue
		}

		require.Equal(t, 1, len(activations))
		require.Equal(t, "a", activations[0].ActorID().ID)
		require.Equal(t, DNSModuleID, activations[0].ModuleID().ID)
		require.Equal(t, "127.0.0.2", activations[0].Address())
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
