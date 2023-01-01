package types

type localRef struct {
	serverID   string
	namespace  string
	actorID    string
	moduleID   string
	generation uint64
}

// NewLocalReferences creates an ActorReference of type ReferenceTypeLocal.
func NewLocalReference(
	serverID string,
	namespace,
	actorID,
	moduleID string,
	generation uint64,
) ActorReference {
	return localRef{
		serverID:   serverID,
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
	return "LOCAL"
}

func (l localRef) Generation() uint64 {
	return l.generation
}
