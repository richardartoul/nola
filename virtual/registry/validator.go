package registry

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/richardartoul/nola/virtual/types"
)

// validator wraps a Registry and ensures that all the arguments to it are
// validated properly. This helps us ensure that validation occurs uniformly
// across all registry implementations.
type validator struct {
	// Don't embed so we know we've overrided every method in the interface
	// explicitly.
	r Registry
}

func newValidatedRegistry(r Registry) Registry {
	return &validator{
		r: r,
	}
}

func (v *validator) RegisterModule(
	ctx context.Context,
	namespace,
	moduleID string,
	moduleBytes []byte,
	opts ModuleOptions,
) (RegisterModuleResult, error) {
	if err := validateString("namespace", namespace); err != nil {
		return RegisterModuleResult{}, err
	}
	if err := validateString("moduleID", moduleID); err != nil {
		return RegisterModuleResult{}, err
	}
	if len(moduleBytes) == 0 && !opts.AllowEmptyModuleBytes {
		return RegisterModuleResult{}, errors.New("moduleBytes must not be empty")
	}
	if len(moduleBytes) > 1<<22 {
		// TODO: 4MiB is a ridiculous low limit, we need to find a way to increase this. Probably
		//       need to store the modules in an external store or just split them across a bunch
		//       of keys in the registry and multiple transactions.
		return RegisterModuleResult{}, fmt.Errorf("moduleBytes must not be > 1<<22, but was: %d", len(moduleBytes))
	}

	// TODO: We could try compiling the WASM bytes here to make sure they're a valid program.

	return v.r.RegisterModule(ctx, namespace, moduleID, moduleBytes, opts)
}

func (v *validator) GetModule(
	ctx context.Context,
	namespace,
	moduleID string,
) ([]byte, ModuleOptions, error) {
	if err := validateString("namespace", namespace); err != nil {
		return nil, ModuleOptions{}, err
	}
	if err := validateString("moduleID", moduleID); err != nil {
		return nil, ModuleOptions{}, err
	}
	return v.r.GetModule(ctx, namespace, moduleID)
}

func (v *validator) CreateActor(
	ctx context.Context,
	namespace,
	actorID,
	moduleID string,
	opts ActorOptions,
) (CreateActorResult, error) {
	if err := validateString("namespace", namespace); err != nil {
		return CreateActorResult{}, err
	}
	if err := validateString("actorID", actorID); err != nil {
		return CreateActorResult{}, err
	}
	if err := validateString("moduleID", moduleID); err != nil {
		return CreateActorResult{}, err
	}

	return v.r.CreateActor(ctx, namespace, actorID, moduleID, opts)
}

func (v *validator) IncGeneration(
	ctx context.Context,
	namespace,
	actorID string,
) error {
	if err := validateString("namespace", namespace); err != nil {
		return err
	}
	if err := validateString("actorID", actorID); err != nil {
		return err
	}
	return v.r.IncGeneration(ctx, namespace, actorID)
}

func (v *validator) EnsureActivation(
	ctx context.Context,
	namespace,
	actorID string,
) ([]types.ActorReference, error) {
	if err := validateString("namespace", namespace); err != nil {
		return nil, err
	}
	if err := validateString("actorID", actorID); err != nil {
		return nil, err
	}
	return v.r.EnsureActivation(ctx, namespace, actorID)
}

func (v *validator) GetVersionStamp(
	ctx context.Context,
) (int64, error) {
	return v.r.GetVersionStamp(ctx)
}

func (v *validator) ActorKVPut(
	ctx context.Context,
	namespace string,
	actorID string,
	key []byte,
	value []byte,
) error {
	if err := validateString("namespace", namespace); err != nil {
		return err
	}
	if err := validateString("actorID", actorID); err != nil {
		return err
	}
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}
	if len(key) > 1<<10 {
		return fmt.Errorf("key cannot be > 1<<10, but was: %d", len(key))
	}
	return v.r.ActorKVPut(ctx, namespace, actorID, key, value)
}

func (v *validator) ActorKVGet(
	ctx context.Context,
	namespace string,
	actorID string,
	key []byte,
) ([]byte, bool, error) {
	if err := validateString("namespace", namespace); err != nil {
		return nil, false, err
	}
	if err := validateString("actorID", actorID); err != nil {
		return nil, false, err
	}
	if len(key) == 0 {
		return nil, false, errors.New("key cannot be empty")
	}
	if len(key) > 1<<10 {
		return nil, false, fmt.Errorf("key cannot be > 1<<10, but was: %d", len(key))
	}
	return v.r.ActorKVGet(ctx, namespace, actorID, key)
}

func (v *validator) Heartbeat(
	ctx context.Context,
	serverID string,
	state HeartbeatState,
) (HeartbeatResult, error) {
	if err := validateString("serverID", serverID); err != nil {
		return HeartbeatResult{}, err
	}
	if err := validateString("address", state.Address); err != nil {
		return HeartbeatResult{}, err
	}
	return v.r.Heartbeat(ctx, serverID, state)
}

func (v *validator) Close(ctx context.Context) error {
	return v.r.Close(ctx)
}

func (v *validator) UnsafeWipeAll() error {
	return v.r.UnsafeWipeAll()
}

func validateString(name, x string) error {
	if x == "" {
		return fmt.Errorf("%s cannot be empty", name)
	}
	if len(x) > 128 {
		return fmt.Errorf("%s cannot be > 128 bytes, but was: %d", name, len(x))
	}

	if len(strings.TrimSpace(x)) != len(x) {
		return fmt.Errorf("%s cannot contain leader or trailing whitespace, but was: %s", name, x)
	}

	return nil
}
