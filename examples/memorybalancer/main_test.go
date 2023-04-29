package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/richardartoul/nola/virtual"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/registry/leaderregistry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"

	"github.com/stretchr/testify/require"
)

const (
	namespace = "memorybalancer-namespace"
	module    = "memory-hog-module"
)

func TestMemoryBalancing(t *testing.T) {
	lp := &leaderProvider{}
	lp.setLeader(net.ParseIP("127.0.0.1"))

	var (
		server1 = newServer(t, lp, 1)
		server2 = newServer(t, lp, 2)
		server3 = newServer(t, lp, 3)
	)

	for i := 0; i < 100; i++ {
		_, err := server1.InvokeActor(
			context.Background(), namespace, fmt.Sprintf("actor-%d", i), module, "keep-alive", nil, types.CreateIfNotExist{})
		require.NoError(t, err)
	}

	// Actors will not necessarily balance equally at first because heartbeating is async so the regsitry has
	// a stale view of the server state at points.
	require.Equal(t, 100, server1.NumActivatedActors()+server2.NumActivatedActors()+server3.NumActivatedActors())

	// Now, make one of the processes use way more memory than the others.
	for {
		_, err := server1.InvokeActor(
			context.Background(), namespace, "actor1", module, "inc-memory-usage", nil, types.CreateIfNotExist{})
		require.NoError(t, err)
		_, err = server2.InvokeActor(
			context.Background(), namespace, "actor2", module, "keep-alive", nil, types.CreateIfNotExist{})
		require.NoError(t, err)
		_, err = server3.InvokeActor(
			context.Background(), namespace, "actor3", module, "keep-alive", nil, types.CreateIfNotExist{})
		require.NoError(t, err)

	}

	// server1
	fmt.Println(server1)
	fmt.Println(server2)
	fmt.Println(server3)
}

func newServer(
	t *testing.T,
	lp leaderregistry.LeaderProvider,
	idx int,
) virtual.Environment {
	var (
		registryServerID = fmt.Sprintf("registry-server-%d", idx)
		envServerID      = fmt.Sprintf("env-server-%d", idx)
		registryPort     = 12000 + idx
		envPort          = 11000 + idx
	)
	reg, err := leaderregistry.NewLeaderRegistry(
		context.Background(), lp, registryServerID, registryPort, virtual.EnvironmentOptions{
			Discovery: virtual.DiscoveryOptions{
				DiscoveryType: virtual.DiscoveryTypeLocalHost,
				Port:          registryPort,
			},
		})
	require.NoError(t, err)

	env, err := virtual.NewEnvironment(
		context.Background(), envServerID, reg, registry.NewNoopModuleStore(), virtual.NewHTTPClient(),
		virtual.EnvironmentOptions{
			Discovery: virtual.DiscoveryOptions{
				DiscoveryType:               virtual.DiscoveryTypeLocalHost,
				Port:                        envPort,
				AllowFailedInitialHeartbeat: true,
			},
			// Need to set this otherwise the environment will detect the address is localhost and just
			// do everythin in-memory which is not what we want since we're trying to simulate a fairly
			// real scenario.
			ForceRemoteProcedureCalls: true,
		})
	require.NoError(t, err)
	require.NoError(t, env.RegisterGoModule(types.NewNamespacedIDNoType(namespace, module), &testModule{}))

	server := virtual.NewServer(registry.NewNoopModuleStore(), env)
	go func() {
		if err := server.Start(envPort); err != nil {
			panic(err)
		}
	}()

	return env
}

type leaderProvider struct {
	sync.Mutex

	leader net.IP
}

func (l *leaderProvider) setLeader(x net.IP) {
	l.Lock()
	defer l.Unlock()

	l.leader = x
}

func (l *leaderProvider) GetLeader() (net.IP, error) {
	l.Lock()
	defer l.Unlock()

	return l.leader, nil
}

type testModule struct {
}

func (tm testModule) Instantiate(
	ctx context.Context,
	reference types.ActorReferenceVirtual,
	payload []byte,
	host virtual.HostCapabilities,
) (virtual.Actor, error) {
	return &testActor{}, nil
}

func (tm testModule) Close(ctx context.Context) error {
	return nil
}

type testActor struct {
	count int
}

func (ta *testActor) MemoryUsageBytes() int {
	return ta.count * 1024
}

func (ta *testActor) Invoke(
	ctx context.Context,
	operation string,
	payload []byte,
	transaction registry.ActorKVTransaction,
) ([]byte, error) {
	switch operation {
	case wapcutils.StartupOperationName:
		return nil, nil
	case wapcutils.ShutdownOperationName:
		return nil, nil
	case "keep-alive":
		return nil, nil
	case "inc-memory-usage":
		ta.count++
		return nil, nil
	default:
		return nil, fmt.Errorf("testActor: unhandled operation: %s", operation)
	}
}

func (ta *testActor) Close(
	ctx context.Context,
) error {
	return nil
}
