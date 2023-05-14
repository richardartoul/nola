package dnsregistry

import (
	"context"
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
	LookupIP(host string) ([]registry.Address, error)
}

type dnsRegistry struct {
	sync.RWMutex

	log *slog.Logger

	// Dependencies.
	resolver DNSResolver
	host     string
	opts     DNSRegistryOptions

	// State.
	addresses []registry.Address
	hashRing  *HashRing

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
	// Logger is a logging instance used for logging messages.
	// If no logger is provided, the default logger from the slog package (slog.Default()) will be used.
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
		resolver = newConstResolver([]registry.Address{{
			IP:   net.ParseIP(LocalAddress),
			Port: port,
		}})
	} else {
		resolver = NewDNSResolver(port)
	}
	return NewDNSRegistryFromResolver(resolver, host, opts)
}

// NewDNSRegistryFromResolver is the same as NewDNSRegistry except it allows
// a custom implementation of DNSResolver to be provided.
func NewDNSRegistryFromResolver(
	resolver DNSResolver,
	host string,
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

func (d *dnsRegistry) EnsureActivation(
	ctx context.Context,
	req registry.EnsureActivationRequest,
) (registry.EnsureActivationResult, error) {
	d.RLock()
	ring := d.hashRing
	d.RUnlock()

	if ring.IsEmpty() {
		return registry.EnsureActivationResult{}, fmt.Errorf(
			"EnsureActivation: hashring is empty")
	}

	serverIP := ring.Get(fmt.Sprintf("%s::%s", req.ActorID, req.ModuleID))
	ref, err := types.NewActorReference(
		DNSServerID, DNSServerVersion, req.Namespace,
		req.ModuleID, req.ActorID, DNS_ACTOR_GENERATION, refPhysicalState{addr: serverIP})
	if err != nil {
		return registry.EnsureActivationResult{}, fmt.Errorf(
			"error creating actor reference: %w", err)
	}

	return registry.EnsureActivationResult{
		References:   []types.ActorReference{ref},
		VersionStamp: DNSVersionStamp,
	}, nil
}

func (d *dnsRegistry) GetVersionStamp(
	ctx context.Context,
) (int64, error) {
	// Must always return 1 because <= 0 is not a legal versionstamp in the system.
	return DNSVersionStamp, nil
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
	d.log.Info("Shutting down")
	close(d.closeCh)
	<-d.closedCh
	d.log.Info("Done shutting down")
	return nil
}

func (d *dnsRegistry) UnsafeWipeAll() error {
	return nil
}

func (d *dnsRegistry) discover() error {
	addresses, err := d.resolver.LookupIP(d.host)
	if err != nil {
		return fmt.Errorf("discover: error looking up IPs: %w", err)
	}

	// crc32.ChecksumIEEE because thats the default groupcache uses
	// https://github.com/golang/groupcache/blob/41bb18bfe9da5321badc438f91158cd790a33aa3/http.go#L72
	// should investigate if we should pick a different value.
	var ()
	hashRing := NewHashRing(64, crc32.ChecksumIEEE)
	ipStrs := make([]string, 0, len(addresses))
	for _, addr := range addresses {
		if addr.IP.To4() != nil {
			ipStrs = append(ipStrs, fmt.Sprintf("%s:%d", addr.IP.To4().String(), addr.Port))
		} else if addr.IP.To16() != nil {
			ipStrs = append(ipStrs, fmt.Sprintf("[%s]:%d", addr.IP.To16().String(), addr.Port))
		} else {
			d.log.Info("[invariant violated] IP is not IP4 or IP6, skipping", slog.Any("ip", addr))
		}
	}
	hashRing.Add(ipStrs...)

	d.Lock()
	oldAddresses := d.addresses
	d.addresses = addresses
	d.hashRing = hashRing
	d.Unlock()

	if len(d.addresses) != len(oldAddresses) {
		d.log.Info(
			"discovered new IP addresses",
			slog.Any("prev", oldAddresses), slog.Any("curr", addresses))
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
	addresses []registry.Address
}

func newConstResolver(addresses []registry.Address) *constResolver {
	return &constResolver{
		addresses: addresses,
	}
}

func (f *constResolver) setIPs(addresses []registry.Address) {
	f.Lock()
	defer f.Unlock()
	f.addresses = addresses
}

func (f *constResolver) LookupIP(host string) ([]registry.Address, error) {
	f.Lock()
	defer f.Unlock()
	return f.addresses, nil
}

type refPhysicalState struct {
	addr string
}

func (s refPhysicalState) NumActivatedActors() int {
	return 0
}

func (s refPhysicalState) UsedMemory() int {
	return 0
}

func (s refPhysicalState) Address() string {
	return s.addr
}
