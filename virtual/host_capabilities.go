package virtual

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"
)

type hostCapabilities struct {
	reg              registry.Registry
	env              Environment
	activations      *activations
	customHostFns    map[string]func([]byte) ([]byte, error)
	reference        types.ActorReferenceVirtual
	getServerStateFn func() (string, int64)
}

func newHostCapabilities(
	reg registry.Registry,
	env Environment,
	activations *activations,
	customHostFns map[string]func([]byte) ([]byte, error),
	reference types.ActorReferenceVirtual,
	getServerStateFn func() (string, int64),
) HostCapabilities {
	return &hostCapabilities{
		reg:              reg,
		env:              env,
		activations:      activations,
		customHostFns:    customHostFns,
		reference:        reference,
		getServerStateFn: getServerStateFn,
	}
}

func (h *hostCapabilities) BeginTransaction(
	ctx context.Context,
) (registry.ActorKVTransaction, error) {
	// Use lazy implementation because we create an implicit transaction for every
	// invocation which would be extremely expensive if it were not for the fact that
	// the transaction is never actually begun unless a KV operation is initiated.
	tr := newLazyActorTransaction(h.reg, h.getServerStateFn, h.reference)
	return tr, nil
}

func (h *hostCapabilities) Transact(
	ctx context.Context,
	fn func(tr registry.ActorKVTransaction) (any, error),
) (any, error) {
	// Use lazy implementation for same reason described in BeginTransaction() above.
	tr := newLazyActorTransaction(h.reg, h.getServerStateFn, h.reference)
	result, err := fn(tr)
	if err != nil {
		tr.Cancel(ctx)
		return nil, fmt.Errorf("hostCapabilities: Transact: %w", err)
	}
	if err := tr.Commit(ctx); err != nil {
		return nil, fmt.Errorf("hostCapabilities: Transact: error commiting transaction: %w", err)
	}
	return result, nil
}

func (h *hostCapabilities) InvokeActor(
	ctx context.Context,
	req types.InvokeActorRequest,
) ([]byte, error) {
	return h.env.InvokeActor(
		ctx, h.reference.Namespace(), req.ActorID, req.ModuleID,
		req.Operation, req.Payload, req.CreateIfNotExist)
}

func (h *hostCapabilities) ScheduleSelfTimer(
	ctx context.Context,
	req wapcutils.ScheduleSelfTimer,
) error {
	// TODO: When the actor gets GC'd (which is not currently implemented), this
	//       timer won't get GC'd with it. We should keep track of all outstanding
	//       timers with the instantiation and terminate them if the actor is
	//       killed.
	time.AfterFunc(time.Duration(req.AfterMillis)*time.Millisecond, func() {
		// Copy the payload to make sure its safe to retain across invocations.
		payloadCopy := make([]byte, len(req.Payload))
		copy(payloadCopy, req.Payload)

		reader, err := h.activations.invoke(
			context.Background(), h.reference, req.Operation, nil, payloadCopy, true)
		if err == nil {
			defer reader.Close()
		}
		if err != nil {
			log.Printf(
				"error firing timer for actor %s, err: %v\n", h.reference, err)
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
		h.reference.Namespace(), operation, payload)
}

// lazyActorTransaction wraps registry.ActorKVTransaction such that the transaction is not
// created / opened unless its actually needed.s
type lazyActorTransaction struct {
	// Dependencies.
	store            registry.ActorStorage
	getServerStateFn func() (string, int64)
	reference        types.ActorReferenceVirtual

	// State.
	//
	// o guards tr and err.
	o   sync.Once
	tr  registry.ActorKVTransaction
	err error
}

func newLazyActorTransaction(
	store registry.ActorStorage,
	getServerStateFn func() (string, int64),
	reference types.ActorReferenceVirtual,
) registry.ActorKVTransaction {
	return &lazyActorTransaction{
		store:            store,
		getServerStateFn: getServerStateFn,
		reference:        reference,
	}
}

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

		serverID, serverVersion := l.getServerStateFn()
		l.tr, l.err = l.store.BeginTransaction(
			ctx,
			l.reference.Namespace(), l.reference.ActorID().ID, l.reference.ModuleID().ID,
			serverID, serverVersion)
	})
	if l.err != nil {
		return fmt.Errorf("maybeInitTr: error beginning lazy transaction: %w", l.err)
	}
	return l.err
}
