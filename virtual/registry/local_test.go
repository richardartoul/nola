package registry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegistrySimple(t *testing.T) {
	registry := NewLocal()

	ctx := context.Background()

	// Create module.
	_, err := registry.RegisterModule(ctx, "ns1", "test-module", []byte("wasm"), ModuleOptions{})
	require.NoError(t, err)

	// Subsequent module for same namespace should fail.
	_, err = registry.RegisterModule(ctx, "ns1", "test-module", []byte("wasm"), ModuleOptions{})
	require.Error(t, err)

	// Succeeds with same module if different namespace.
	_, err = registry.RegisterModule(ctx, "ns2", "test-module", []byte("wasm"), ModuleOptions{})
	require.NoError(t, err)

	// Create actor fails for unknown module.
	_, err = registry.CreateActor(ctx, "ns1", "a", "unknown-module", ActorOptions{})
	require.Error(t, err)

	// Succeeds for known module.
	_, err = registry.CreateActor(ctx, "ns1", "a", "test-module", ActorOptions{})
	require.NoError(t, err)

	// Fails to create duplicate actor in same namespace.
	_, err = registry.CreateActor(ctx, "ns1", "a", "test-module", ActorOptions{})
	require.Error(t, err)

	// Allows actors with same ID in different namespaces.
	_, err = registry.CreateActor(ctx, "ns2", "a", "test-module", ActorOptions{})
	require.NoError(t, err)
}

func TestKVSimple(t *testing.T) {
	registry := NewLocal()

	ctx := context.Background()

	// Neither PUT nor GET should work until an actor exists.
	err := registry.ActorKVPut(ctx, "ns1", "a", []byte("key1"), []byte("hello world"))
	require.Error(t, err)
	_, _, err = registry.ActorKVGet(ctx, "ns1", "a", []byte("key1"))
	require.Error(t, err)

	// Create the module/actor.
	_, err = registry.RegisterModule(ctx, "ns1", "test-module", []byte("wasm"), ModuleOptions{})
	require.NoError(t, err)
	_, err = registry.CreateActor(ctx, "ns1", "a", "test-module", ActorOptions{})
	require.NoError(t, err)

	// PUT/GET should work now.
	_, ok, err := registry.ActorKVGet(ctx, "ns1", "a", []byte("key1"))
	require.NoError(t, err)
	// key1 should not exist yet.
	require.False(t, ok)

	// Store key1 now. Subsequent GET should work.
	err = registry.ActorKVPut(ctx, "ns1", "a", []byte("key1"), []byte("hello world"))
	require.NoError(t, err)

	val, ok, err := registry.ActorKVGet(ctx, "ns1", "a", []byte("key1"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("hello world"), val)
}
