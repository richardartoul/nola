package types

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
