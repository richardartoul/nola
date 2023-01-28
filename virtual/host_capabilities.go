package virtual

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/wapcutils"
)

type hostCapabilities struct {
	reg           registry.Registry
	env           Environment
	customHostFns map[string]func([]byte) ([]byte, error)
	namespace     string
	actorID       string
	actorModuleID string
}

func newHostCapabilities(
	reg registry.Registry,
	env Environment,
	customHostFns map[string]func([]byte) ([]byte, error),
	namespace string,
	actorID string,
	actorModuleID string,
) HostCapabilities {
	return &hostCapabilities{
		reg:           reg,
		env:           env,
		customHostFns: customHostFns,
		namespace:     namespace,
		actorID:       actorID,
		actorModuleID: actorModuleID,
	}
}

func (h *hostCapabilities) BeginTransaction(
	ctx context.Context,
) (registry.ActorKVTransaction, error) {
	tr, err := h.reg.BeginTransaction(ctx, h.namespace, h.actorID)
	if err != nil {
		return nil, fmt.Errorf("hostCapabilities: beginTransaction: error creating transaction: %w", err)
	}
	return tr, nil
}

// func (h *hostCapabilities) Put(
// 	ctx context.Context,
// 	key []byte,
// 	value []byte,
// ) error {
// 	return h.reg.ActorKVPut(ctx, h.namespace, h.actorID, key, value)
// }

// func (h *hostCapabilities) Get(
// 	ctx context.Context,
// 	key []byte,
// ) ([]byte, bool, error) {
// 	return h.reg.ActorKVGet(ctx, h.namespace, h.actorID, key)
// }

func (h *hostCapabilities) CreateActor(
	ctx context.Context,
	req wapcutils.CreateActorRequest,
) (CreateActorResult, error) {
	if req.ModuleID == "" {
		// If no module ID was specified then assume the actor is trying to "fork"
		// itself and create the new actor using the same module as the existing
		// actor.
		req.ModuleID = h.actorModuleID
	}

	_, err := h.reg.CreateActor(ctx, h.namespace, req.ActorID, req.ModuleID, registry.ActorOptions{})
	if err != nil {
		return CreateActorResult{}, err
	}
	return CreateActorResult{}, nil
}

func (h *hostCapabilities) InvokeActor(
	ctx context.Context,
	req wapcutils.InvokeActorRequest,
) ([]byte, error) {
	return h.env.InvokeActor(ctx, h.namespace, req.ActorID, req.Operation, req.Payload)
}

func (h *hostCapabilities) ScheduleInvokeActor(
	ctx context.Context,
	req wapcutils.ScheduleInvocationRequest,
) error {
	if req.Invoke.ActorID == "" {
		// Omitted if the actor wants to schedule a delayed invocation (timer) for itself.
		req.Invoke.ActorID = h.actorID
	}

	// TODO: When the actor gets GC'd (which is not currently implemented), this
	//       timer won't get GC'd with it. We should keep track of all outstanding
	//       timers with the instantiation and terminate them if the actor is
	//       killed.
	time.AfterFunc(time.Duration(req.AfterMillis)*time.Millisecond, func() {
		// Copy the payload to make sure its safe to retain across invocations.
		payloadCopy := make([]byte, len(req.Invoke.Payload))
		copy(payloadCopy, req.Invoke.Payload)
		_, err := h.env.InvokeActor(ctx, h.namespace, req.Invoke.ActorID, req.Invoke.Operation, payloadCopy)
		if err != nil {
			log.Printf(
				"error performing scheduled invocation from actor: %s to actor: %s for operation: %s, err: %v\n",
				h.actorID, req.Invoke.ActorID, req.Invoke.Operation, err)
		}
	})

	return nil
}

func (h *hostCapabilities) CustomFn(
	ctx context.Context,
	operation string,
	payload []byte,
) ([]byte, error) {
	customFn, ok := h.customHostFns[operation]
	if ok {
		res, err := customFn(payload)
		if err != nil {
			return nil, fmt.Errorf("error running custom host function: %s, err: %w", operation, err)
		}
		return res, nil
	}
	return nil, fmt.Errorf(
		"unknown host function: %s::%s::%s",
		h.namespace, operation, payload)
}

// lazyActorTransaction wraps registry.ActorKVTransaction such that the transaction is not
// created / opened unless its actually needed.s
type lazyActorTransaction struct {
	// Dependencies.
	store     registry.ActorStorage
	namespace string
	actorID   string

	// State.
	//
	// o guards tr and err.
	o   sync.Once
	tr  registry.ActorKVTransaction
	err error
}

func newLazyActorTransaction(
	store registry.ActorStorage,
	namespace string,
	actorID string,
) registry.ActorKVTransaction {
	return &lazyActorTransaction{
		store:     store,
		namespace: namespace,
		actorID:   actorID,
	}
}

// Put(ctx context.Context, key []byte, value []byte) error
// 	Get(ctx context.Context, key []byte) ([]byte, bool, error)
// 	Commit(ctx context.Context) error
// 	Cancel(ctx context.Context) error

func (l *lazyActorTransaction) Put(
	ctx context.Context,
	key, value []byte,
) error {
	if err := l.maybeInitTr(ctx, true); err != nil {
		return fmt.Errorf("lazyActorTransaction: Put: error initializing transaction: %w", err)
	}

	if err := l.tr.Put(ctx, key, value); err != nil {
		return fmt.Errorf("lazyActorTransaction: Put: error calling Put: %w", err)
	}

	return nil
}

func (l *lazyActorTransaction) Get(
	ctx context.Context,
	key []byte,
) ([]byte, bool, error) {
	if err := l.maybeInitTr(ctx, true); err != nil {
		return nil, false, fmt.Errorf(
			"lazyActorTransaction: Put: error initializing transaction: %w", err)
	}

	v, ok, err := l.tr.Get(ctx, key)
	if err != nil {
		return nil, false, fmt.Errorf(
			"lazyActorTransaction: Get: error calling Get: %w", err)
	}

	return v, ok, nil
}

func (l *lazyActorTransaction) Commit(ctx context.Context) error {
	if err := l.maybeInitTr(ctx, false); err != nil {
		return fmt.Errorf(
			"[invariant violated] lazyActorTransaction: Commit: error initializing transaction: %w", err)
	}
	if l.tr == nil {
		return nil
	}

	if err := l.tr.Commit(ctx); err != nil {
		return fmt.Errorf("lazyActorTransaction: Commit: error committing: %w", err)
	}

	return nil
}

func (l *lazyActorTransaction) Cancel(ctx context.Context) error {
	if err := l.maybeInitTr(ctx, false); err != nil {
		return fmt.Errorf(
			"[invariant violated] lazyActorTransaction: Cancel: error canceling transaction: %w", err)
	}
	if l.tr == nil {
		return nil
	}

	if err := l.tr.Cancel(ctx); err != nil {
		return fmt.Errorf("lazyActorTransaction: Cancel: error canceling: %w", err)
	}

	return nil
}

func (l *lazyActorTransaction) maybeInitTr(
	ctx context.Context,
	createIfNotExist bool,
) error {
	l.o.Do(func() {
		if !createIfNotExist {
			return
		}

		l.tr, l.err = l.store.BeginTransaction(ctx, l.namespace, l.actorID)
	})
	if l.err != nil {
		return fmt.Errorf("maybeInitTr: error beginning lazy transaction: %w", l.err)
	}
	return l.err
}
