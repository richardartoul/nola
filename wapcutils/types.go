package wapcutils

const (
	// KVPutOperationName is the string that indicates the operation in WAPC is a KV PUT.
	KVPutOperationName = "KV-PUT"
	// KVGetOperationName is the string that indicates the operation in WAPC is a KV GET.
	KVGetOperationName = "KV-GET"
	// CreateActorOperationName is the string that indicates the operation in WAPC is to
	// create a new actor.
	CreateActorOperationName = "CREATE-ACTOR"
	// InvokeActorOperationName is the string that indicates the operation in WAPC is to
	// invoke an operation (function) on another actor.
	InvokeActorOperationName = "INVOKE-ACTOR"
	// StartupOperationName is the string that indicates the operation in WAPC is the
	// startup function which should be run once when a worker/actor is first loaded into
	// memory.
	StartupOperationName = "STARTUP"
	// ShutdownOperationName is the string that indicates the operation in WAPC is the
	// shutdown function which should be run once right before a worker/actor is evicted
	// from memory.
	ShutdownOperationName = "SHUTDOWN"
)
