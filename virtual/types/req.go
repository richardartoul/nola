package types

// InvokeActorRequest is the JSON struct that represents a request from an existing
// actor to invoke an operation on another one.
type InvokeActorRequest struct {
	// ActorID is the ID of the target actor.
	ActorID string `json:"actor_id"`
	// ModuleID is the ID of the module for which the actor should be activated.
	ModuleID string `json:"module_id"`
	// Operation is the name of the operation to invoke on the target actor.
	Operation string `json:"operation"`
	// Payload is the []byte payload to provide to the invoked function on the
	// target actor.
	Payload []byte `json:"payload"`
	// CreateIfNotExist provides the arguments for InvokeActorRequest to construct the
	// actor if it doesn't already exist. This field is optional.
	CreateIfNotExist CreateIfNotExist `json:"create_if_not_exist"`
}

// CreateIfNotExist provides the arguments for InvokeActorRequest to construct the
// actor if it doesn't already exist.
type CreateIfNotExist struct {
	Options ActorOptions `json:"actor_options"`
	// InstantiatePayload is the []byte that will be provided to the actor on
	// instantiation. It is generally used to provide any actor-specific constructor
	// arguments that are required to instantiate the actor in memory.
	InstantiatePayload []byte
}

// ActorOptions contains the options for a given actor.
type ActorOptions struct {
	// ExtraReplicas represents the number of additional replicas requested for an actor.
	// It specifies the desired number of replicas, in addition to the primary replica,
	// that should be created during actor activation.
	// The value of ExtraReplicas should be a non-negative integer.
	ExtraReplicas uint64 `json:"extra_replicas"`
	// RetryPolicy specifies the retry policy for actor invocations.
	// It defines the behavior when an invocation fails.
	// Possible values are:
	// - "retry_never": The invocation will not be retried.
	// - "retry_if_replica_available": The invocation will be retried on other available replicas.
	RetryPolicy RetryPolicy `json:"retry_policy"`
}

// RetryPolicy defines the retry policies for actor invocations.
type RetryPolicy string

const (
	// RetryIfReplicaAvailable specifies that an invocation should be retried on other available replicas if it fails.
	RetryIfReplicaAvailable RetryPolicy = "if_replica_available"

	// RetryNever specifies that an invocation should not be retried.
	RetryNever RetryPolicy = "never"
)
