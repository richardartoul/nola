package dnsregistry

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"sync"
	"time"

	"golang.org/x/exp/slog"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
)

const (
	// Localhost should be provided to the DNSRegistry constructor as the
	// value for host when using the DNSRegistry in single-node implementations,
	// in-meomry implementations, or tests.
	Localhost    = "localhost"
	LocalAddress = "127.0.0.1"

	DNSServerID          = "DNS_SERVER_ID"
	DNSServerVersion     = int64(-1)
	DNS_ACTOR_GENERATION = 1
	// Must be at least 1 because <= 0 is not a legal versionstamp
	DNSVersionStamp = 1
)

// DNSResolver is the interface that must be implemented by a resolver
// in order to map a hostname to set of live IPs.
type DNSResolver interface {
	LookupIP(host string) ([]net.IP, error)
}

type dnsRegistry struct {
	sync.RWMutex

	log *slog.Logger

	// Dependencies.
	resolver DNSResolver
	host     string
	port     int
	opts     DNSRegistryOptions

	// State.
	ips      []net.IP
	hashRing *HashRing

	// Shutdown logic.
	discoveryRunning bool
	closeCh          chan struct{}
	closedCh         chan struct{}
}

// DNSRegistryOptions contains the options for the DNS resgistry
// implementation.
type DNSRegistryOptions struct {
	// ResolveEvery controls how often the LookupIP method will be
	// called on the DNSResolver to detect which IPs are active.
	ResolveEvery time.Duration
	// Logger is the logger. If no logger is passed, then default slog.Default() is used.
	Logger *slog.Logger
}

// NewDNSRegistry creates a new registry.Registry backed by DNS.
func NewDNSRegistry(
	host string,
	port int,
	opts DNSRegistryOptions,
) (registry.Registry, error) {
	var resolver DNSResolver
	if host == Localhost {
		resolver = newConstResolver([]net.IP{net.ParseIP(LocalAddress)})
	} else {
		resolver = NewDNSResolver()
	}
	return NewDNSRegistryFromResolver(resolver, host, port, opts)
}

// NewDNSRegistryFromResolver is the same as NewDNSRegistry except it allows
// a custom implementation of DNSResolver to be provided.
func NewDNSRegistryFromResolver(
	resolver DNSResolver,
	host string,
	port int,
	opts DNSRegistryOptions,
) (registry.Registry, error) {
	if opts.ResolveEvery == 0 {
		opts.ResolveEvery = 5 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	d := &dnsRegistry{
		log:      opts.Logger.With(slog.String("module", "Registry"), slog.String("subService", "dnsRegistry")),
		resolver: resolver,
		host:     host,
		port:     port,
		opts:     opts,

		closeCh:  make(chan struct{}),
		closedCh: make(chan struct{}),
	}

	if err := d.discover(); err != nil {
		return nil, fmt.Errorf(
			"NewDNSRegistry: error looking up IPs for name: %s, err: %w",
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
	opts registry.ModuleOptions,
) (registry.RegisterModuleResult, error) {
	return registry.RegisterModuleResult{}, nil
}

// GetModule gets the bytes and options associated with the provided module.
func (d *dnsRegistry) GetModule(
	ctx context.Context,
	namespace,
	moduleID string,
) ([]byte, registry.ModuleOptions, error) {
	return nil, registry.ModuleOptions{}, nil
}

func (d *dnsRegistry) CreateActor(
	ctx context.Context,
	namespace,
	actorID,
	moduleID string,
	opts types.ActorOptions,
) (registry.CreateActorResult, error) {
	return registry.CreateActorResult{}, nil
}

func (d *dnsRegistry) IncGeneration(
	ctx context.Context,
	namespace,
	actorID string,
	moduleID string,
) error {
	return errors.New("DNSRegistry: IncGeneration: not implemented")
}

func (d *dnsRegistry) EnsureActivation(
	ctx context.Context,
	namespace,
	actorID string,
	moduleID string,
) ([]types.ActorReference, error) {
	d.RLock()
	ring := d.hashRing
	d.RUnlock()

	if ring.IsEmpty() {
		return nil, fmt.Errorf("EnsureActivation: hashring is empty")
	}

	serverIP := ring.Get(fmt.Sprintf("%s::%s", actorID, moduleID))
	ref, err := types.NewActorReference(
		DNSServerID, DNSServerVersion, serverIP, namespace,
		moduleID, actorID, DNS_ACTOR_GENERATION)
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
	moduleID string,
	serverID string,
	serverVersion int64,
) (_ registry.ActorKVTransaction, err error) {
	return nil, errors.New("DNSRegistry: BeginTransaction: not implemented")
}

func (d *dnsRegistry) Heartbeat(
	ctx context.Context,
	serverID string,
	heartbeatState registry.HeartbeatState,
) (registry.HeartbeatResult, error) {
	return registry.HeartbeatResult{
		VersionStamp: DNSVersionStamp,
		// Must be at least 1 so heartbeat.Versionstamp + TTL > DNSVersionStamp
		HeartbeatTTL:  1,
		ServerVersion: DNSServerVersion,
	}, nil
}

func (d *dnsRegistry) Close(ctx context.Context) error {
	d.log.Info("DNSRegistry: Shutting down")
	close(d.closeCh)
	<-d.closedCh
	d.log.Info("DNSRegistry: Done shutting down")
	return nil
}

func (d *dnsRegistry) UnsafeWipeAll() error {
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
		if ip.To4() != nil {
			ipStrs = append(ipStrs, fmt.Sprintf("%s:%d", ip.To4().String(), d.port))
		} else if ip.To16() != nil {
			ipStrs = append(ipStrs, fmt.Sprintf("[%s]:%d", ip.To16().String(), d.port))
		} else {
			d.log.Info("[invariant violated] IP is not IP4 or IP6, skipping", slog.Any("ip", ip))
		}
	}
	hashRing.Add(ipStrs...)

	d.Lock()
	oldIPs := d.ips
	d.ips = ips
	d.hashRing = hashRing
	d.Unlock()

	if len(d.ips) != len(oldIPs) {
		d.log.Info("discovered new IP addresses", slog.Any("prev", oldIPs), slog.Any("curr", ips))
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
				d.log.Error("discoveryLoop: error performing background discovery", slog.Any("error", err))
			}
		case <-d.closeCh:
			return
		}
	}
}

type constResolver struct {
	sync.Mutex
	ips []net.IP
}

func newConstResolver(ips []net.IP) *constResolver {
	return &constResolver{
		ips: ips,
	}
}

func (f *constResolver) setIPs(ips []net.IP) {
	f.Lock()
	defer f.Unlock()
	f.ips = ips
}

func (f *constResolver) LookupIP(host string) ([]net.IP, error) {
	f.Lock()
	defer f.Unlock()
	return f.ips, nil
}
