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
