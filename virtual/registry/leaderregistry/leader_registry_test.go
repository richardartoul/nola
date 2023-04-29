package leaderregistry

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/richardartoul/nola/virtual"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/registry/dnsregistry"

	"github.com/stretchr/testify/require"
)

func TestLeaderRegistry(t *testing.T) {
	registry.TestAllCommon(t, func() registry.Registry {
		lp := newTestLeaderProvider()
		lp.setLeader(net.ParseIP(dnsregistry.LocalAddress))

		envOpts := virtual.EnvironmentOptions{Discovery: virtual.DiscoveryOptions{
			DiscoveryType: virtual.DiscoveryTypeLocalHost,
			Port:          9093,
		}}

		reg, err := NewLeaderRegistry(context.Background(), lp, "server1", 9093, envOpts)
		require.NoError(t, err)

		return reg
	})
}

type testLeaderProvider struct {
	sync.Mutex
	leader net.IP
}

func newTestLeaderProvider() *testLeaderProvider {
	return &testLeaderProvider{}
}

func (lp *testLeaderProvider) GetLeader() (net.IP, error) {
	lp.Lock()
	defer lp.Unlock()

	return lp.leader, nil
}

func (lp *testLeaderProvider) setLeader(ip net.IP) {
	lp.Lock()
	defer lp.Unlock()
	lp.leader = ip
}
