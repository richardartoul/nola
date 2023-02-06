package registry

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/richardartoul/nola/virtual/types"
)

type dnsResolver interface {
	LookupIP(host string) ([]net.IP, error)
}

type dnsRegistry struct {
}

func NewDNSRegistry(
	resolver dnsResolver,
) (Registry, error) {
	ips, err := resolver.LookupIP("google.com")
	if err != nil {
		return nil, fmt.Errorf("DNSRegister: error looking up IPs for name: %s", "google.com")
	}
	for _, ip := range ips {
		fmt.Printf("google.com. IN A %s\n", ip.String())
	}
	return &dnsRegistry{}, errors.New("wtf")
}

func (d *dnsRegistry) RegisterModule(
	ctx context.Context,
	namespace,
	moduleID string,
	moduleBytes []byte,
	opts ModuleOptions,
) (RegisterModuleResult, error) {
	return RegisterModuleResult{}, errors.New("DNSRegistry: RegisterModule: not implemented")
}

// GetModule gets the bytes and options associated with the provided module.
func (d *dnsRegistry) GetModule(
	ctx context.Context,
	namespace,
	moduleID string,
) ([]byte, ModuleOptions, error) {
	return nil, ModuleOptions{}, errors.New("DNSRegistry: GetModule: not implemented")
}

func (d *dnsRegistry) CreateActor(
	ctx context.Context,
	namespace,
	actorID,
	moduleID string,
	opts types.ActorOptions,
) (CreateActorResult, error) {
	return CreateActorResult{}, errors.New("DNSRegistry: CreateActor: not implemented")
}

func (d *dnsRegistry) IncGeneration(
	ctx context.Context,
	namespace,
	actorID string,
) error {
	return errors.New("DNSRegistry: IncGeneration: not implemented")
}

func (d *dnsRegistry) EnsureActivation(
	ctx context.Context,
	namespace,
	actorID string,
) ([]types.ActorReference, error) {
	return nil, errors.New("DNSRegistry: EnsureActivation: not implemented")
}

func (d *dnsRegistry) GetVersionStamp(
	ctx context.Context,
) (int64, error) {
	// TODO: Implement me.
	return -1, errors.New("DNSRegistry: GetVersionStamp: not implemented")
}

func (d *dnsRegistry) BeginTransaction(
	ctx context.Context,
	namespace string,
	actorID string,

	serverID string,
	serverVersion int64,
) (_ ActorKVTransaction, err error) {
	return nil, errors.New("DNSRegistry: BeginTransaction: not implemented")
}

func (d *dnsRegistry) Heartbeat(
	ctx context.Context,
	serverID string,
	heartbeatState HeartbeatState,
) (HeartbeatResult, error) {
	// TODO: Implement me.
	return HeartbeatResult{}, errors.New("DNSRegistry: Heartbeat: not implemented")
}

func (d *dnsRegistry) Close(ctx context.Context) error {
	// TODO: Implement me.
	return nil
}

func (d *dnsRegistry) UnsafeWipeAll() error {
	// TODO: Implement me.
	return nil
}
