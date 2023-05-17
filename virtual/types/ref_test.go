package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewActorReference(t *testing.T) {
	ref, err := NewActorReference("server1", 0, "a", "b", "c", 1, ServerState{Address: "server1path"})
	require.NoError(t, err)

	require.Equal(t, "server1", ref.Physical.ServerID)
	require.Equal(t, "server1path", ref.Physical.ServerID)
	require.Equal(t, "a", ref.Virtual.Namespace)
	require.Equal(t, "c", ref.Virtual.ActorID)
	require.Equal(t, "b", ref.Virtual.ModuleID)
	require.Equal(t, uint64(1), ref.Virtual.Generation)
	require.Equal(t, IDTypeActor, ref.Virtual.IDType)

	marshaled, err := json.Marshal(ref)
	require.NoError(t, err)

	unmarshaled, err := NewActorReferenceFromJSON(marshaled)
	require.NoError(t, err)
	require.Equal(t, ref, unmarshaled)

}

func TestNewWorkerReference(t *testing.T) {
	ref, err := NewActorReference("server1", 0, "a", "b", "c", 1, ServerState{Address: "server1path"})
	require.NoError(t, err)
	ref.Virtual.IDType = IDTypeWorker

	require.Equal(t, "server1", ref.Physical.ServerID)
	require.Equal(t, "server1path", ref.Physical.ServerState.Address)
	require.Equal(t, "a", ref.Virtual.Namespace)
	require.Equal(t, "b", ref.Virtual.ModuleID)
	require.Equal(t, "c", ref.Virtual.ActorID)
	require.Equal(t, uint64(1), ref.Virtual.Generation)
	require.Equal(t, IDTypeWorker, ref.Virtual.IDType)

	marshaled, err := json.Marshal(ref)
	require.NoError(t, err)

	unmarshaled, err := NewActorReferenceFromJSON(marshaled)
	require.NoError(t, err)
	require.Equal(t, ref, unmarshaled)
}
