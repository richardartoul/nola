package types

type localRef struct {
	serverID   string
	address    string
	namespace  string
	actorID    string
	moduleID   string
	generation uint64
}

// NewLocalReferences creates an ActorReference of type ReferenceTypeLocal.
func NewLocalReference(
	serverID,
	address,
	namespace,
	actorID,
	moduleID string,
	generation uint64,
) ActorReference {
	return localRef{
		serverID:   serverID,
		address:    address,
		namespace:  namespace,
		actorID:    actorID,
		moduleID:   moduleID,
		generation: generation,
	}
}

func (l localRef) Type() ReferenceType {
	return ReferenceTypeLocal
}

func (l localRef) ServerID() string {
	return l.serverID
}

func (l localRef) Namespace() string {
	return l.namespace
}

func (l localRef) ActorID() NamespacedID {
	return NewNamespacedID(l.namespace, l.actorID)
}

func (l localRef) ModuleID() NamespacedID {
	return NewNamespacedID(l.namespace, l.moduleID)
}

func (l localRef) Address() string {
	return l.address
}

func (l localRef) Generation() uint64 {
	return l.generation
}
