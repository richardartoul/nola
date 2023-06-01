package types

import (
	"fmt"
	"time"
)

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

// Validate validates that the CreateIfNotExist struct is valid.
func (o *CreateIfNotExist) Validate() error {
	if err := o.Options.Validate(); err != nil {
		return fmt.Errorf("error validating CreateIfNotExist")
	}
	return nil
}

// ActorOptions contains the options for a given actor.
type ActorOptions struct {
	// ExtraReplicas represents the number of additional replicas requested for an actor.
	// It specifies the desired number of replicas, in addition to the primary replica,
	// that should be created during actor activation.
	// The value of ExtraReplicas should be a non-negative integer.
	ExtraReplicas uint64 `json:"extra_replicas"`

	// ReplicationStrategy specifies the retry selection strategy for replica invocations.
	// It determines how retry attempts are distributed among replicas.
	// Note: Retries are only performed on available replicas.
	ReplicationStrategy ReplicaSelectionStrategy `json:"replica_selection_strategy"`
	// RetryPolicy specifies the retry policy for actor invocations.
	// It defines the behavior when an invocation fails.
	RetryPolicy RetryPolicy `json:"retry_policy"`
}

// Validate validates that the ActorOptions struct is valid.
func (o *ActorOptions) Validate() error {
	if o.ReplicationStrategy == ReplicaSelectionStrategyBroadcast &&
		o.ExtraReplicas > 0 &&
		o.RetryPolicy.PerAttemptTimeout <= 0 {
		return fmt.Errorf("PerAttemptTimeout must be > 0 when using ReplicaSelectionStrategyBroadcast")
	}

	if o.RetryPolicy.PerAttemptTimeoutGrowthMultiplier > 0 && o.RetryPolicy.PerAttemptTimeoutGrowthMultiplier < 1 {
		return fmt.Errorf("RetryPolicy.PerAttemptTimeoutGrowthMultiplier must be >= 1")
	}

	return nil
}

// ReplicaSelectionStrategy defines the available replica selection strategies for actor invocations.
type ReplicaSelectionStrategy string

const (
	// ReplicaSelectionStrategyRandom indicates that replication attempts will be distributed
	// randomly across replicas.
	ReplicaSelectionStrategyRandom ReplicaSelectionStrategy = "random"

	// ReplicaSelectionStrategySorted indicates that replication attempts will be
	// biased towards one replica until it breaks.
	ReplicaSelectionStrategySorted ReplicaSelectionStrategy = "sorted"

	// ReplicaSelectionStrategyBroadcast indicates that replication attempts will
	// be broadcast concurrently to all replicas.
	ReplicaSelectionStrategyBroadcast ReplicaSelectionStrategy = "broadcast"
)

// RetryPolicy defines the retry policies for actor invocations.
// It specifies the strategy, per-attempt timeout, and maximum number of retries.
type RetryPolicy struct {
	// PerAttemptTimeout defines the timeout duration for each retry attempt.
	// If a single retry attempt exceeds this duration, it will be considered a failure.
	// Note: If the `PerAttemptTimeout` is set to 0,
	// it uses the parent context timeout and there is no specific per-attempt timeout.
	PerAttemptTimeout time.Duration `json:"per_attempt_timeout"`

	// PerAttemptTimeoutGrowthMultiplier controls how much the PerAttemptTimeout should
	// grow after each failed attempt.
	PerAttemptTimeoutGrowthMultiplier float64 `json:"per_attempt_timeout_growth_multiplier"`

	// MaxNumRetries sets the maximum number of retries allowed for the actor invocation.
	// After reaching this limit, the invocation will fail if no successful response is received.
	// Note: Retries are limited by the availability of replicas.
	MaxNumRetries uint `json:"max_num_retries"`
}
