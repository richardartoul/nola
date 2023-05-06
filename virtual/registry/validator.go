package registry

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	// Make sure validator implements ModuleStore as well.
	_ ModuleStore = &validator{}
)

// validator wraps a Registry and ensures that all the arguments to it are
// validated properly. This helps us ensure that validation occurs uniformly
// across all registry implementations.
type validator struct {
	// Don't embed so we know we've overrided every method in the interface
	// explicitly.
	r Registry
}

// NewValidatedRegistry wraps the provided Registry r such that it validates
// inputs before delegating calls. This makes it easier to write new registry
// implementations without making all of them re-implement the validation
// logic.
func NewValidatedRegistry(r Registry) Registry {
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
	moduleStore, ok := v.r.(ModuleStore)
	if !ok {
		return RegisterModuleResult{}, errors.New("registry does not implement RegisterModule")
	}

	if err := validateString("namespace", namespace); err != nil {
		return RegisterModuleResult{}, err
	}
	if err := validateString("moduleID", moduleID); err != nil {
		return RegisterModuleResult{}, err
	}
	if len(moduleBytes) == 0 {
		return RegisterModuleResult{}, errors.New("moduleBytes must not be empty")
	}
	if len(moduleBytes) > 1<<22 {
		// TODO: 4MiB is a ridiculous low limit, we need to find a way to increase this. Probably
		//       need to store the modules in an external store or just split them across a bunch
		//       of keys in the registry and multiple transactions.
		return RegisterModuleResult{}, fmt.Errorf("moduleBytes must not be > 1<<22, but was: %d", len(moduleBytes))
	}

	// TODO: We could try compiling the WASM bytes here to make sure they're a valid program.
	return moduleStore.RegisterModule(ctx, namespace, moduleID, moduleBytes, opts)
}

func (v *validator) GetModule(
	ctx context.Context,
	namespace,
	moduleID string,
) ([]byte, ModuleOptions, error) {
	moduleStore, ok := v.r.(ModuleStore)
	if !ok {
		return nil, ModuleOptions{}, errors.New("registry does not implement GetModule")
	}

	if err := validateString("namespace", namespace); err != nil {
		return nil, ModuleOptions{}, err
	}
	if err := validateString("moduleID", moduleID); err != nil {
		return nil, ModuleOptions{}, err
	}
	return moduleStore.GetModule(ctx, namespace, moduleID)
}

func (v *validator) EnsureActivation(
	ctx context.Context,
	req EnsureActivationRequest,
) (EnsureActivationResult, error) {
	if err := validateString("namespace", req.Namespace); err != nil {
		return EnsureActivationResult{}, err
	}
	if err := validateString("actorID", req.ActorID); err != nil {
		return EnsureActivationResult{}, err
	}
	return v.r.EnsureActivation(ctx, req)
}

func (v *validator) GetVersionStamp(
	ctx context.Context,
) (int64, error) {
	return v.r.GetVersionStamp(ctx)
}

func (v *validator) BeginTransaction(
	ctx context.Context,
	namespace string,
	actorID string,
	moduleID string,
	serverID string,
	serverVersion int64,
) (ActorKVTransaction, error) {
	if err := validateString("namespace", namespace); err != nil {
		return nil, err
	}
	if err := validateString("actorID", namespace); err != nil {
		return nil, err
	}

	tr, err := v.r.BeginTransaction(ctx, namespace, actorID, moduleID, serverID, serverVersion)
	if err != nil {
		return nil, err
	}

	return &kvValidator{tr}, nil
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

type kvValidator struct {
	tr ActorKVTransaction
}

func (k *kvValidator) Put(ctx context.Context, key []byte, value []byte) error {
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}
	if len(key) > 1<<10 {
		return fmt.Errorf("key cannot be > 1<<10, but was: %d", len(key))
	}

	return k.tr.Put(ctx, key, value)
}

func (k *kvValidator) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, errors.New("key cannot be empty")
	}
	if len(key) > 1<<10 {
		return nil, false, fmt.Errorf("key cannot be > 1<<10, but was: %d", len(key))
	}

	return k.tr.Get(ctx, key)
}

func (k *kvValidator) Commit(ctx context.Context) error {
	return k.tr.Commit(ctx)
}

func (k *kvValidator) Cancel(ctx context.Context) error {
	return k.tr.Cancel(ctx)
}
