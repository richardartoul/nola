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
		lp.setLeader(registry.Address{
			IP:   net.ParseIP(dnsregistry.LocalAddress),
			Port: 9093,
		})

		envOpts := virtual.EnvironmentOptions{Discovery: virtual.DiscoveryOptions{
			DiscoveryType: virtual.DiscoveryTypeLocalHost,
			Port:          9093,
		}}

		reg, err := NewLeaderRegistry(context.Background(), lp, "server1", envOpts)
		require.NoError(t, err)

		return reg
	})
}

type testLeaderProvider struct {
	sync.Mutex
	leader registry.Address
}

func newTestLeaderProvider() *testLeaderProvider {
	return &testLeaderProvider{}
}

func (lp *testLeaderProvider) GetLeader() (registry.Address, error) {
	lp.Lock()
	defer lp.Unlock()

	return lp.leader, nil
}

func (lp *testLeaderProvider) setLeader(addr registry.Address) {
	lp.Lock()
	defer lp.Unlock()
	lp.leader = addr
}
