package wapcutils

// CreateActorRequest is the JSON struct that represents a request from an existing
// actor to create a new one.
type CreateActorRequest struct {
	// ActorID is the requested ID for the new actor that should be created.
	ActorID string `json:"actor_id"`
	// ModuleID is the ID of the module from which the new actor should be created. If
	// this is empty then it will default to the module ID of the calling actor. This
	// allows actors to "fork" themselves without being aware of their own module ID
	// as a convenience.
	ModuleID string `json:"module_id"`
}

// InvokeActorRequest is the JSON struct that represents a request from an existing
// actor to invoke an operation on another one.
type InvokeActorRequest struct {
	// ActorID is the ID of the target actor. Omit when being used inside of
	// ScheduleInvocationRequest to target self.
	ActorID string `json:"actor_id"`
	// Operation is the name of the operation to invoke on the target actor.
	Operation string `json:"operation"`
	// Payload is the []byte payload to provide to the invoked function on the
	// target actor.
	Payload []byte `json:"payload"`
}

// ScheduleInvocationRequest is the JSON struct that represents a request from an
// existing actor to invoke an operation on another one (or its self) at a later
// time.
type ScheduleInvocationRequest struct {
	Invoke      InvokeActorRequest `json:"invocation"`
	AfterMillis int                `json:"after_millis"`
}
