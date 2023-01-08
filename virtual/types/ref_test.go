package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewActorReference(t *testing.T) {
	ref, err := NewActorReference("server1", "server1path", "a", "b", "c", 1)
	require.NoError(t, err)

	require.Equal(t, "server1", ref.ServerID())
	require.Equal(t, "server1path", ref.Address())
	require.Equal(t, "a", ref.Namespace())
	require.Equal(t, "a", ref.ActorID().Namespace)
	require.Equal(t, "c", ref.ActorID().ID)
	require.Equal(t, "a", ref.ModuleID().Namespace)
	require.Equal(t, "b", ref.ModuleID().ID)
	require.Equal(t, uint64(1), ref.Generation())
}
