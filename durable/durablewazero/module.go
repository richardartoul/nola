package durablewazero

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/richardartoul/nola/durable"
	"github.com/wapc/wapc-go"
)

type module struct {
	sync.Mutex
	m         wapc.Module
	instances map[string]wapc.Instance
}

func NewModule(
	ctx context.Context,
	engine wapc.Engine,
	host func(ctx context.Context, binding, namespace, operation string, payload []byte) ([]byte, error),
	guestModuleBytes []byte,
) (durable.Module, error) {
	m, err := engine.New(ctx, host, guestModuleBytes, &wapc.ModuleConfig{
		Logger: wapc.PrintlnLogger,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating new wazero module from engine: %w", err)
	}

	return &module{
		m:         m,
		instances: make(map[string]wapc.Instance),
	}, nil
}

func (m *module) Instantiate(
	ctx context.Context,
	id string,
) (durable.Object, error) {
	m.Lock()
	defer m.Unlock()

	_, ok := m.instances[id]
	if ok {
		return nil, fmt.Errorf("instance with ID: %s already exists", id)
	}

	instance, err := m.m.Instantiate(ctx)
	if err != nil {
		return nil, fmt.Errorf("module: error instantiating instance: %w", err)
	}
	m.instances[id] = instance

	return newObject(instance, func() {
		m.Lock()
		defer m.Unlock()
		delete(m.instances, id)
	}), nil
}

func (d *module) Close(ctx context.Context) error {
	d.Lock()
	defer d.Unlock()
	if len(d.instances) > 0 {
		return fmt.Errorf("module: Close: cannot close with > 0 unclosed instances")
	}
	return d.m.Close(ctx)
}
