package registry

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"sync"
	"time"

	"github.com/richardartoul/nola/virtual/types"
)

const (
	DNSServerID          = "DNS_SERVER_ID"
	DNSServerVersion     = int64(-1)
	DNSModuleID          = "DNS_MODULE_ID"
	DNS_ACTOR_GENERATION = 1
	// Must be at least 1 because <= 0 is not a legal versionstamp
	DNSVersionStamp = 1
)

type dnsResolver interface {
	LookupIP(host string) ([]net.IP, error)
}

type dnsRegistry struct {
	sync.RWMutex

	// Dependencies.
	resolver dnsResolver
	host     string
	port     int64
	opts     DNSRegistryOptions

	// State.
	ips      []net.IP
	hashRing *HashRing

	// Shutdown logic.
	discoveryRunning bool
	closeCh          chan struct{}
	closedCh         chan struct{}
}

type DNSRegistryOptions struct {
	ResolveEvery time.Duration
}

func NewDNSRegistry(
	resolver dnsResolver,
	host string,
	port int64,
	opts DNSRegistryOptions,
) (Registry, error) {
	if opts.ResolveEvery == 0 {
		opts.ResolveEvery = 5 * time.Second
	}

	d := &dnsRegistry{
		resolver: resolver,
		host:     host,
		port:     port,
		opts:     opts,

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
	return RegisterModuleResult{}, nil
}

// GetModule gets the bytes and options associated with the provided module.
func (d *dnsRegistry) GetModule(
	ctx context.Context,
	namespace,
	moduleID string,
) ([]byte, ModuleOptions, error) {
	return nil, ModuleOptions{}, nil
}

func (d *dnsRegistry) CreateActor(
	ctx context.Context,
	namespace,
	actorID,
	moduleID string,
	opts types.ActorOptions,
) (CreateActorResult, error) {
	return CreateActorResult{}, nil
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
	d.RLock()
	ring := d.hashRing
	d.RUnlock()

	if ring.IsEmpty() {
		return nil, fmt.Errorf("EnsureActivation: hashring is empty")
	}

	serverIP := ring.Get(actorID)
	ref, err := types.NewActorReference(
		DNSServerID, DNSServerVersion, serverIP, namespace,
		DNSModuleID, actorID, DNS_ACTOR_GENERATION)
	if err != nil {
		return nil, fmt.Errorf("error creating actor reference: %w", err)
	}

	return []types.ActorReference{ref}, nil

}

func (d *dnsRegistry) GetVersionStamp(
	ctx context.Context,
) (int64, error) {
	// Must always return 1 because <= 0 is not a legal versionstamp in the system.
	return DNSVersionStamp, nil
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
	return HeartbeatResult{
		VersionStamp: DNSVersionStamp,
		// Must be at least 1 so heartbeat.Versionstamp + TTL > DNSVersionStamp
		HeartbeatTTL:  1,
		ServerVersion: DNSServerVersion,
	}, nil
}

func (d *dnsRegistry) Close(ctx context.Context) error {
	log.Printf("DNSRegistry: Shutting down")
	close(d.closeCh)
	<-d.closedCh
	log.Printf("DNSRegistry: Done shutting down")
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

	// crc32.ChecksumIEEE because thats the default groupcache uses
	// https://github.com/golang/groupcache/blob/41bb18bfe9da5321badc438f91158cd790a33aa3/http.go#L72
	// should investigate if we should pick a different value.
	var ()
	hashRing := NewHashRing(64, crc32.ChecksumIEEE)
	ipStrs := make([]string, 0, len(ips))
	for _, ip := range ips {
		ipStrs = append(ipStrs, fmt.Sprintf("%s:%d", ip.To4().String(), d.port))
	}
	hashRing.Add(ipStrs...)

	d.Lock()
	oldIPs := d.ips
	d.ips = ips
	d.hashRing = hashRing
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
	ticker := time.NewTicker(d.opts.ResolveEvery)
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
