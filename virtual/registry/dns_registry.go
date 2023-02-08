package registry

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/richardartoul/nola/virtual/types"
)

type dnsResolver interface {
	LookupIP(host string) ([]net.IP, error)
}

type dnsRegistry struct {
	sync.Mutex

	resolver dnsResolver
	host     string
	ips      []net.IP

	discoveryRunning bool
	closeCh          chan struct{}
	closedCh         chan struct{}
}

func NewDNSRegistry(
	resolver dnsResolver,
	host string,
) (Registry, error) {
	d := &dnsRegistry{
		resolver: resolver,
		host:     host,

		closeCh:  make(chan struct{}),
		closedCh: make(chan struct{}),
	}

	if err := d.discover(); err != nil {
		return nil, fmt.Errorf(
			"NewDNSRegistry: error looking up UPs for name: %s, err: %w",
			host, err)
	}

	go d.discoveryLoop()

	return d, nil
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

func (d *dnsRegistry) discover() error {
	ips, err := d.resolver.LookupIP(d.host)
	if err != nil {
		return fmt.Errorf("discover: error looking up IPs: %w", err)
	}

	d.Lock()
	oldIPs := d.ips
	d.ips = ips
	d.Unlock()

	if len(d.ips) != len(oldIPs) {
		log.Printf(
			"DNSRegistry: discovered new IP addresses: prev: %v, curr: %v\n",
			oldIPs, ips)
	}

	return nil
}

func (d *dnsRegistry) discoveryLoop() {
	d.Lock()
	if d.discoveryRunning {
		d.Unlock()
		panic("[invariant violated] discovery already running")
	}
	d.Unlock()

	defer close(d.closedCh)
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			if err := d.discover(); err != nil {
				log.Printf("discoveryLoop: error performing background discovery: %v\n", err)
			}
		case <-d.closeCh:
			return
		}
	}
}
