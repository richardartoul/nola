package registry

import (
	"context"
	"errors"
)

type noopModuleStore struct {
}

// IsNoopModuleStore returns a boolean indicating whether the provided ModuelStore is
// backed by noopModuleStore. This is only used for producing better error messages.
func IsNoopModuleStore(m ModuleStore) bool {
	_, ok := m.(*noopModuleStore)
	return ok
}

// NewNoopModuleStore returns a new ModuleStore that returns an error for every method call.
func NewNoopModuleStore() ModuleStore {
	return &noopModuleStore{}
}

func (n *noopModuleStore) RegisterModule(
	ctx context.Context,
	namespace,
	moduleID string,
	moduleBytes []byte,
	opts ModuleOptions,
) (RegisterModuleResult, error) {
	return RegisterModuleResult{}, errors.New("noopModuleStore: RegisterModule not implemented")
}

// GetModule gets the bytes and options associated with the provided module.
func (n *noopModuleStore) GetModule(
	ctx context.Context,
	namespace,
	moduleID string,
) ([]byte, ModuleOptions, error) {
	return nil, ModuleOptions{}, errors.New("noopModuleStore: GetModule not implemented")
}
