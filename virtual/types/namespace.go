package types

import (
	"fmt"
	"strings"
)

// NamespacedID wraps an ID with its namespace. This is useful
// for namespacing strings, for example when they're used as
// map keys.
type NamespacedID struct {
	Namespace string
	ID        string
	IDType    string
}

// NewNamespacedID creates a NamespacedID from the provided namespace
// and and ID.
func NewNamespacedID(
	namespace string,
	id string,
	idType string,
) NamespacedID {
	return NamespacedID{
		Namespace: namespace,
		ID:        id,
		IDType:    idType,
	}
}

// NamespacedActorID is the same as NamespacedID, but for actor IDs and
// includes the ModuleID.
type NamespacedActorID struct {
	Namespace string
	ID        string
	Module    string
	IDType    string
}

// NewNamespacedActorID creates a new NamespaceActorID.
func NewNamespacedActorID(
	namespace string,
	id string,
	module string,
	idType string,
) NamespacedActorID {
	return NamespacedActorID{
		Namespace: namespace,
		ID:        id,
		Module:    module,
		IDType:    idType,
	}
}

func (n *NamespacedActorID) Less(b NamespacedActorID) int {
	nsCmp := strings.Compare(n.Namespace, b.Namespace)
	if nsCmp != 0 {
		return nsCmp
	}

	moduleCmp := strings.Compare(n.Module, b.Module)
	if moduleCmp != 0 {
		return moduleCmp
	}

	idTypeCmp := strings.Compare(n.IDType, b.IDType)
	if idTypeCmp != 0 {
		return idTypeCmp
	}

	return strings.Compare(n.ID, b.ID)
}

func (n *NamespacedActorID) String() string {
	return fmt.Sprintf(
		"%s::%s::%s:%s",
		n.Namespace, n.Module, n.IDType, n.ID)
}

// NamespacedIDNoType is the same as NamespacedID, but without the IDType
// field.
type NamespacedIDNoType struct {
	Namespace string
	ID        string
}

// NewNamespacedIDNoType creates a new NamespacedIDNoType.
func NewNamespacedIDNoType(
	namespace string,
	id string,
) NamespacedIDNoType {
	return NamespacedIDNoType{
		Namespace: namespace,
		ID:        id,
	}
}
