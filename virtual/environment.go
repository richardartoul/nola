package virtual

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"

	"github.com/dgraph-io/ristretto"
)

const (
	heartbeatTimeout           = registry.HeartbeatTTL
	defaultActivationsCacheTTL = heartbeatTimeout
	maxNumActivationsToCache   = 1e6 // 1 Million.
)

type environment struct {
	// State.
	activations     *activations // Internally synchronized.
	activationCache *ristretto.Cache

	heartbeatState struct {
		sync.RWMutex
		registry.HeartbeatResult
		frozen bool
	}

	// Closed when the background heartbeating goroutine should be shut down.
	closeCh chan struct{}
	// Closed when the background heartbeating goroutine completes shutting down.
	closedCh chan struct{}

	// Dependencies.
	serverID string
	address  string
	registry registry.Registry
	client   RemoteClient
	opts     EnvironmentOptions

	paused bool
}

const (
	// DiscoveryTypeLocalHost indicates that the environment should advertise its IP
	// address as localhost to the discovery service.
	DiscoveryTypeLocalHost = "localhost"
	// DiscoveryTypeRemote indicates that the environment should advertise its
	// actual IP to the discovery service.
	DiscoveryTypeRemote = "remote"
)

// DiscoveryOptions contains the discovery-related options.
type DiscoveryOptions struct {
	// DiscoveryType is one of DiscoveryTypeLocalHost or DiscoveryTypeRemote.
	DiscoveryType string
	// Port is the port that the environment should advertise to the discovery
	// service.
	Port int
}

func (d *DiscoveryOptions) Validate() error {
	if d.DiscoveryType != DiscoveryTypeLocalHost &&
		d.DiscoveryType != DiscoveryTypeRemote {
		return fmt.Errorf("unknown discovery type: %v", d.DiscoveryType)
	}
	if d.Port == 0 && d.DiscoveryType != DiscoveryTypeLocalHost {
		return errors.New("port cannot be zero")
	}

	return nil
}

// EnvironmentOptions is the settings for the Environment.
type EnvironmentOptions struct {
	// ActivationCacheTTL is the TTL of the activation cache.
	ActivationCacheTTL time.Duration
	// DisableActivationCache disables the activation cache.
	DisableActivationCache bool
	// Discovery contains the discovery options.
	Discovery DiscoveryOptions
}

// NewEnvironment creates a new Environment.
func NewEnvironment(
	ctx context.Context,
	serverID string,
	reg registry.Registry,
	client RemoteClient,
	opts EnvironmentOptions,
) (Environment, error) {
	if opts.ActivationCacheTTL == 0 {
		opts.ActivationCacheTTL = defaultActivationsCacheTTL
	}

	activationCache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: maxNumActivationsToCache * 10, // * 10 per the docs.
		// Maximum number of entries in cache (~1million). Note that
		// technically this is a measure in bytes, but we pass a cost of 1
		// always to make it behave as a limit on number of activations.
		MaxCost: 1e6,
		// Recommended default.
		BufferItems: 64,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating activationCache: %w", err)
	}

	host := "localhost"
	if opts.Discovery.DiscoveryType == DiscoveryTypeRemote {
		selfIP, err := getSelfIP()
		if err != nil {
			return nil, fmt.Errorf("error discovering self IP: %w", err)
		}
		host = selfIP.To4().String()
	}
	address := fmt.Sprintf("%s:%d", host, opts.Discovery.Port)

	env := &environment{
		activationCache: activationCache,
		closeCh:         make(chan struct{}),
		closedCh:        make(chan struct{}),
		registry:        reg,
		client:          client,
		address:         address,
		serverID:        serverID,
		opts:            opts,
	}
	activations := newActivations(reg, env)
	env.activations = activations

	log.Printf("registering self with address: %s", address)

	// Do one heartbeat right off the bat so the environment is immediately useable.
	err = env.heartbeat()
	if err != nil {
		return nil, fmt.Errorf("failed to perform initial heartbeat: %w", err)
	}

	localEnvironmentsRouterLock.Lock()
	defer localEnvironmentsRouterLock.Unlock()
	if _, ok := localEnvironmentsRouter[address]; ok {
		return nil, fmt.Errorf("tried to register: %s to local environemnt router twice", address)
	}
	localEnvironmentsRouter[address] = env

	go func() {
		defer close(env.closedCh)
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				if env.paused {
					return
				}
				if err := env.heartbeat(); err != nil {
					log.Printf("error performing background heartbeat: %v\n", err)
				}
			case <-env.closeCh:
				log.Printf(
					"environment with serverID: %s and address: %s is shutting down\n",
					env.serverID, env.address)
				return
			}
		}
	}()

	return env, nil
}

var bufPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, 128)
	},
}

func (r *environment) InvokeActor(
	ctx context.Context,
	namespace string,
	actorID string,
	operation string,
	payload []byte,
) ([]byte, error) {
	vs, err := r.registry.GetVersionStamp(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting version stamp: %w", err)
	}

	bufIface := bufPool.Get()
	defer bufPool.Put(bufIface)
	cacheKey := bufPool.Get().([]byte)[:0]
	cacheKey = append(cacheKey, []byte(namespace)...)
	cacheKey = append(cacheKey, []byte(actorID)...)

	var (
		references  []types.ActorReference
		referencesI any = nil
		ok              = false
	)
	if !r.opts.DisableActivationCache {
		referencesI, ok = r.activationCache.Get(cacheKey)
	}
	if ok {
		references = referencesI.([]types.ActorReference)
	} else {
		var err error
		// TODO: Need a concurrency limiter on this thing.
		references, err = r.registry.EnsureActivation(ctx, namespace, actorID)
		if err != nil {
			return nil, fmt.Errorf(
				"error ensuring activation of actor: %s in registry: %w",
				actorID, err)
		}

		// Note that we need to copy the cache key before we call Set() since it will be
		// returned to the pool when this function returns.
		cacheKeyClone := append([]byte(nil), cacheKey...)

		// Set a TTL on the cache entry so that if the generation count increases
		// it will eventually get reflected in the system even if its not immediate.
		// Note that the purpose the generation count is is for code/setting upgrades
		// so it does not need to take effect immediately.
		r.activationCache.SetWithTTL(cacheKeyClone, references, 1, r.opts.ActivationCacheTTL)
	}
	if len(references) == 0 {
		return nil, fmt.Errorf(
			"ensureActivation() success with 0 references for actor ID: %s", actorID)
	}

	return r.invokeReferences(ctx, vs, references, operation, payload)
}

func (r *environment) InvokeActorDirect(
	ctx context.Context,
	versionStamp int64,
	serverID string,
	serverVersion int64,
	reference types.ActorReferenceVirtual,
	operation string,
	payload []byte,
) ([]byte, error) {
	if serverID == "" {
		return nil, errors.New("serverID cannot be empty")
	}
	if serverID != r.serverID {
		return nil, fmt.Errorf(
			"request for serverID: %s received by server: %s, cannot fullfil",
			serverID, r.serverID)
	}
	if versionStamp <= 0 {
		return nil, fmt.Errorf("versionStamp must be >= 0, but was: %d", versionStamp)
	}

	// TODO: Delete me, but useful for now.
	log.Printf("%d::%s:%s::%s::%s\n", versionStamp, serverID, reference.ModuleID().ID, reference.ActorID().ID, operation)

	r.heartbeatState.RLock()
	heartbeatResult := r.heartbeatState.HeartbeatResult
	r.heartbeatState.RUnlock()

	if heartbeatResult.VersionStamp+heartbeatResult.HeartbeatTTL < versionStamp {
		return nil, fmt.Errorf(
			"InvokeLocal: server heartbeat(%d) + TTL(%d) < versionStamp(%d)",
			heartbeatResult.VersionStamp, heartbeatResult.HeartbeatTTL, versionStamp)
	}

	if heartbeatResult.ServerVersion != serverVersion {
		return nil, fmt.Errorf(
			"InvokeLocal: server version(%d) != server version from reference(%d)",
			heartbeatResult.ServerVersion, serverVersion)
	}

	return r.activations.invoke(ctx, reference, operation, payload)
}

func (r *environment) InvokeWorker(
	ctx context.Context,
	namespace string,
	moduleID string,
	operation string,
	payload []byte,
) ([]byte, error) {
	// TODO: The implementation of this function is nice because it just reusees a bunch of the
	//       actor logic. However, it's also less performant than it could be because it still
	//       effectively makes worker execution single-threaded per-server. We should add the
	//       ability for multiple workers of the same module ID to execute in parallel on a
	//       single server. This should be relatively straightforward to do with a few modications
	//       to activations.go.
	ref, err := types.NewVirtualWorkerReference(namespace, moduleID, moduleID)
	if err != nil {
		return nil, fmt.Errorf("InvokeWorker: error creating actor reference: %w", err)
	}

	// Workers provide none of the consistency / linearizability guarantees that actor's do, so we
	// can bypass the registry entirely and just immediately invoke the function.
	return r.activations.invoke(ctx, ref, operation, payload)
}

func (r *environment) Close() error {
	// TODO: This should call Close on the activations field (which needs to be implemented).

	localEnvironmentsRouterLock.Lock()
	delete(localEnvironmentsRouter, r.address)
	localEnvironmentsRouterLock.Unlock()

	close(r.closeCh)
	<-r.closedCh

	return nil
}

func (r *environment) numActivatedActors() int {
	return r.activations.numActivatedActors()
}

func (r *environment) heartbeat() error {
	ctx, cc := context.WithTimeout(context.Background(), heartbeatTimeout)
	defer cc()
	result, err := r.registry.Heartbeat(ctx, r.serverID, registry.HeartbeatState{
		NumActivatedActors: r.numActivatedActors(),
		Address:            r.address,
	})
	if err != nil {
		return fmt.Errorf("error heartbeating: %w", err)
	}

	r.heartbeatState.Lock()
	if !r.heartbeatState.frozen {
		r.heartbeatState.HeartbeatResult = result
	}
	r.heartbeatState.Unlock()
	return nil
}

// TODO: This is kind of a giant hack, but it's really only used for testing. The idea is that
// even when we're using local references, we still want to be able to create multiple
// environments in memory that can all "route" to each other. To accomplish this, everytime an
// environment is created in memory we added it to this global map. Once it is closed, we remove it.
var (
	localEnvironmentsRouter     map[string]Environment = map[string]Environment{}
	localEnvironmentsRouterLock sync.RWMutex
)

func (r *environment) invokeReferences(
	ctx context.Context,
	versionStamp int64,
	references []types.ActorReference,
	operation string,
	payload []byte,
) ([]byte, error) {
	// TODO: Load balancing or some other strategy if the number of references is > 1?
	ref := references[0]
	localEnvironmentsRouterLock.RLock()
	localEnv, ok := localEnvironmentsRouter[ref.Address()]
	localEnvironmentsRouterLock.RUnlock()
	if ok {
		return localEnv.InvokeActorDirect(ctx, versionStamp, ref.ServerID(), ref.ServerVersion(), ref, operation, payload)
	}
	return r.client.InvokeActorRemote(ctx, versionStamp, ref, operation, payload)
}

func (r *environment) freezeHeartbeatState() {
	r.heartbeatState.Lock()
	r.heartbeatState.frozen = true
	r.heartbeatState.Unlock()
}

func (r *environment) pauseHeartbeat() {
	r.paused = true
}

func (r *environment) resumeHeartbeat() {
	r.paused = false
}

func getSelfIP() (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, address := range addrs {
		var ip net.IP
		switch v := address.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			continue // loopback and IsLinkLocalUnicast interface
		}

		ip = ip.To4()
		if ip == nil {
			continue // not an ipv4 address
		}

		return ip, nil
	}

	return nil, errors.New("could not discovery self IPV4")
}
