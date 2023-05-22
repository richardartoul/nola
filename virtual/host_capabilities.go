package virtual

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/exp/slog"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"
)

type hostCapabilities struct {
	log              *slog.Logger
	reg              registry.Registry
	env              Environment
	activations      *activations
	customHostFns    map[string]func([]byte) ([]byte, error)
	reference        types.ActorReferenceVirtual
	getServerStateFn func() (string, int64)
}

func newHostCapabilities(
	log *slog.Logger,
	reg registry.Registry,
	env Environment,
	activations *activations,
	customHostFns map[string]func([]byte) ([]byte, error),
	reference types.ActorReferenceVirtual,
	getServerStateFn func() (string, int64),
) HostCapabilities {
	return &hostCapabilities{
		log:              log.With(slog.String("module", "HostCapabilities")),
		reg:              reg,
		env:              env,
		activations:      activations,
		customHostFns:    customHostFns,
		reference:        reference,
		getServerStateFn: getServerStateFn,
	}
}

func (h *hostCapabilities) InvokeActor(
	ctx context.Context,
	req types.InvokeActorRequest,
) ([]byte, error) {
	return h.env.InvokeActor(
		ctx, h.reference.Namespace, req.ActorID, req.ModuleID,
		req.Operation, req.Payload, req.CreateIfNotExist)
}

func (h *hostCapabilities) ScheduleSelfTimer(
	ctx context.Context,
	req wapcutils.ScheduleSelfTimer,
) error {
	// Copy the payload to make sure its safe to retain across invocations.
	payloadCopy := make([]byte, len(req.Payload))
	copy(payloadCopy, req.Payload)

	// TODO: When the actor gets GC'd this timer won't get GC'd with it. We
	// should keep track of all outstanding timers with the instantiation and
	// terminate them if the actor is killed, but its fine for now.
	time.AfterFunc(time.Duration(req.AfterMillis)*time.Millisecond, func() {
		reader, err := h.activations.invoke(
			context.Background(), h.reference, req.Operation, nil, payloadCopy, true)
		if err == nil {
			// This is weird, but the reader can be nil in the case where the actor the timer is
			// associated with is no longer activated in memory anymore.
			//
			// TestScheduleSelfTimersAndGC intentionally triggers this behavior to ensure that timers
			// never trigger reactivation of deactivated actors.
			if reader != nil {
				defer reader.Close()
			}
		}
		if err != nil {
			h.log.Error("error firing timer for actor", slog.Any("actor", h.reference), slog.Any("error", err))
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
		h.reference.Namespace, operation, payload)
}
