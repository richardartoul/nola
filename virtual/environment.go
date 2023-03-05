package virtual

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"sync"
	"time"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/registry/dnsregistry"
	"github.com/richardartoul/nola/virtual/types"

	"github.com/dgraph-io/ristretto"
)

const (
	Localhost = "127.0.0.1"

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
		paused bool
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
}

const (
	// DiscoveryTypeLocalHost indicates that the environment should advertise its IP
	// address as localhost to the discovery service.
	DiscoveryTypeLocalHost = "localhost"
	// DiscoveryTypeRemote indicates that the environment should advertise its
	// actual IP to the discovery service.
	DiscoveryTypeRemote = "remote"
)

// EnvironmentOptions is the settings for the Environment.
type EnvironmentOptions struct {
	// ActivationCacheTTL is the TTL of the activation cache.
	ActivationCacheTTL time.Duration
	// DisableActivationCache disables the activation cache.
	DisableActivationCache bool
	// Discovery contains the discovery options.
	Discovery DiscoveryOptions
	// ForceRemoteProcedureCalls forces the environment to *always* invoke
	// actors via RPC even if the actor is activated on the node
	// that originally received the request.
	ForceRemoteProcedureCalls bool

	// // GoModules contains a set of Modules implemented in Go (instead of
	// // WASM). This is useful when using NOLA as a library.
	// GoModules map[types.NamespacedIDNoType]Module
	// CustomHostFns contains a set of additional user-defined host
	// functions that can be exposed to activated actors. This allows
	// developeres leveraging NOLA as a library to extend the environment
	// with additional host functionality.
	CustomHostFns map[string]func([]byte) ([]byte, error)
}

// NewDNSRegistryEnvironment is a convenience function that creates a virtual environment backed
// by a DNS-based registry. It is configured with reasonable defaults that make it suitable for
// production usage. Note that this convenience function is particularly nice because it can also
// be used for unit/integration tests and local development simply by passing virtual.Localhost
// as the value of host.
func NewDNSRegistryEnvironment(
	ctx context.Context,
	host string,
	port int,
	opts EnvironmentOptions,
) (Environment, registry.Registry, error) {
	opts.Discovery.Port = port
	if host == Localhost || host == dnsregistry.LocalAddress {
		opts.Discovery.DiscoveryType = DiscoveryTypeLocalHost
	} else {
		opts.Discovery.DiscoveryType = DiscoveryTypeRemote
	}

	if err := opts.Validate(); err != nil {
		return nil, nil, fmt.Errorf("error validating EnvironmentOptions: %w", err)
	}

	reg, err := dnsregistry.NewDNSRegistry(host, port, dnsregistry.DNSRegistryOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("error creating DNS registry: %w", err)
	}

	env, err := NewEnvironment(ctx, dnsregistry.DNSServerID, reg, NewHTTPClient(), opts)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating new virtual environment: %w", err)
	}

	return env, reg, nil
}

// NewTestDNSRegistryEnvironment is a convenience function that creates a virtual environment
// backed by a DNS-based registry. It is configured already to generate a suitable setting up
// for writing unit/integration tests, but not for production usage.
func NewTestDNSRegistryEnvironment(
	ctx context.Context,
	opts EnvironmentOptions,
) (Environment, registry.Registry, error) {
	return NewDNSRegistryEnvironment(ctx, Localhost, 9093, opts)
}

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

func (e *EnvironmentOptions) Validate() error {
	if err := e.Discovery.Validate(); err != nil {
		return fmt.Errorf("error validating discovery options: %w", err)
	}
	return nil
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

	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("error validating EnvironmentOptions: %w", err)
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

	host := Localhost
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
	activations := newActivations(reg, env, env.opts.CustomHostFns)
	env.activations = activations

	// Skip confusing log if dnsregistry is being used since it doesn't use the registry-based
	// registration mechanism in the traditional way.
	if serverID != dnsregistry.DNSServerID {
		log.Printf("registering self with address: %s", address)
	}

	// Do one heartbeat right off the bat so the environment is immediately useable.
	err = env.heartbeat()
	if err != nil {
		return nil, fmt.Errorf("failed to perform initial heartbeat: %w", err)
	}

	localEnvironmentsRouterLock.Lock()
	defer localEnvironmentsRouterLock.Unlock()
	if _, ok := localEnvironmentsRouter[address]; ok {
		return nil, fmt.Errorf("tried to register: %s to local environment router twice", address)
	}
	localEnvironmentsRouter[address] = env

	go func() {
		defer close(env.closedCh)
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				if env.heartbeatState.paused {
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

func (r *environment) RegisterGoModule(id types.NamespacedIDNoType, module Module) error {
	// Register all the GoModules in the registry so they're useable with calls to
	// CreateActor() and EnsureActivation().
	//
	// Note that its safe to do this on every node in the cluster because the registry will
	// ignore duplicate RegisterModule() calls if AllowEmptyModuleBytes is set to true.
	_, err := r.registry.RegisterModule(context.TODO(), id.Namespace, id.ID, nil, registry.ModuleOptions{
		AllowEmptyModuleBytes: true,
	})
	if err != nil {
		return fmt.Errorf("failed to register go module with ID: %v, err: %w", id, err)
	}

	// After registering the Module with the registry, we also need to register it with the
	// activations datastructure so it knows how to handle subsequent activations/invocations.
	return r.activations.registerGoModule(id, module)
}

func (r *environment) InvokeActor(
	ctx context.Context,
	namespace string,
	actorID string,
	moduleID string,
	operation string,
	payload []byte,
	create types.CreateIfNotExist,
) ([]byte, error) {
	reader, err := r.InvokeActorStream(
		ctx, namespace, actorID, moduleID, operation, payload, create)
	if err != nil {
		return nil, fmt.Errorf("error invoking actor: %w", err)
	}

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading actor response from stream: %w", err)
	}

	return b, nil
}

func (r *environment) InvokeActorStream(
	ctx context.Context,
	namespace string,
	actorID string,
	moduleID string,
	operation string,
	payload []byte,
	create types.CreateIfNotExist,
) (io.ReadCloser, error) {
	if namespace == "" {
		return nil, errors.New("InvokeActor: namespace cannot be empty")
	}
	if actorID == "" {
		return nil, errors.New("InvokeActor: actorID cannot be empty")
	}
	if moduleID == "" {
		return nil, errors.New("InvokeActor: moduleID cannot be empty")
	}

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
		references, err = r.registry.EnsureActivation(ctx, namespace, actorID, moduleID)
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

	return r.invokeReferences(ctx, vs, references, operation, payload, create)
}

func (r *environment) InvokeActorDirect(
	ctx context.Context,
	versionStamp int64,
	serverID string,
	serverVersion int64,
	reference types.ActorReferenceVirtual,
	operation string,
	payload []byte,
	create types.CreateIfNotExist,
) ([]byte, error) {
	reader, err := r.InvokeActorDirectStream(
		ctx, versionStamp, serverID, serverVersion,
		reference, operation, payload, create)
	if err != nil {
		return nil, fmt.Errorf("error invoking actor: %w", err)
	}

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading actor response from stream: %w", err)
	}

	return b, nil
}

func (r *environment) InvokeActorDirectStream(
	ctx context.Context,
	versionStamp int64,
	serverID string,
	serverVersion int64,
	reference types.ActorReferenceVirtual,
	operation string,
	payload []byte,
	create types.CreateIfNotExist,
) (io.ReadCloser, error) {
	if serverID == "" {
		return nil, errors.New("serverID cannot be empty")
	}
	if serverID != r.serverID && serverID != dnsregistry.DNSServerID {
		// Make sure the client has reached the server it intended. This is an important
		// check due to the limitations of I.P-based network addressing. For example, if
		// two pods of NOLA were running in k8s on a shared set of VMs and get
		// rescheduled such that they switch I.P addresses, clients may temporarily route
		// requests for server A to server B and vice versa.
		//
		// Note that we also skip this check if the requested serverID is the hard-coded
		// registry.DNSServerID value. In this scenario, the application is using a DNS
		// based registry solution and everything is "loose" in terms of correctness anyways
		// so it doesn't matter if we sometimes reach the wrong physical server in rare
		// cases.
		//
		// TODO: Technically the server should return its identity in the response and the
		//       client should assert on that as well to avoid issues where the request
		//       reaches the wrong application entirely and that application just returns
		//       OK to everything.
		return nil, fmt.Errorf(
			"request for serverID: %s received by server: %s, cannot fullfil",
			serverID, r.serverID)
	}
	if versionStamp <= 0 {
		return nil, fmt.Errorf("versionStamp must be >= 0, but was: %d", versionStamp)
	}

	// TODO: Delete me, but useful for now.
	// log.Printf("%d::%s:%s::%s::%s\n", versionStamp, serverID, reference.ModuleID().ID, reference.ActorID().ID, operation)

	r.heartbeatState.RLock()
	heartbeatResult := r.heartbeatState.HeartbeatResult
	r.heartbeatState.RUnlock()

	if heartbeatResult.VersionStamp+heartbeatResult.HeartbeatTTL < versionStamp {
		return nil, fmt.Errorf(
			"InvokeLocal: server heartbeat(%d) + TTL(%d) < versionStamp(%d)",
			heartbeatResult.VersionStamp, heartbeatResult.HeartbeatTTL, versionStamp)
	}

	// Compare server version of this environment to the server version from the actor activation reference to ensure
	// the env hasn't missed a heartbeat recently, which could cause it to lose ownership of the actor.
	// This bug was identified using this mode.l https://github.com/richardartoul/nola/blob/master/proofs/stateright/activation-cache/README.md
	if heartbeatResult.ServerVersion != serverVersion {
		return nil, fmt.Errorf(
			"InvokeLocal: server version(%d) != server version from reference(%d)",
			heartbeatResult.ServerVersion, serverVersion)
	}

	return r.activations.invoke(ctx, reference, operation, create.InstantiatePayload, payload)
}

func (r *environment) InvokeWorker(
	ctx context.Context,
	namespace string,
	moduleID string,
	operation string,
	payload []byte,
	create types.CreateIfNotExist,
) ([]byte, error) {
	reader, err := r.InvokeWorkerStream(
		ctx, namespace, moduleID, operation, payload, create)
	if err != nil {
		return nil, fmt.Errorf("error invoking worker: %w", err)
	}

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading worker response from stream: %w", err)
	}

	return b, nil
}

func (r *environment) InvokeWorkerStream(
	ctx context.Context,
	namespace string,
	moduleID string,
	operation string,
	payload []byte,
	create types.CreateIfNotExist,
) (io.ReadCloser, error) {
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
	return r.activations.invoke(ctx, ref, operation, create.InstantiatePayload, payload)
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

	// Ensure the latest ServerVersion is set on the activation struct as well so
	// that new calls to BeginTransaction() in the registry from actors will have
	// the most up-to-date ServerVersion, otherwise they could begin failing at
	// some point.
	r.activations.setServerState(r.serverID, result.ServerVersion)
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
	create types.CreateIfNotExist,
) (io.ReadCloser, error) {
	// TODO: Load balancing or some other strategy if the number of references is > 1?
	ref := references[0]
	if !r.opts.ForceRemoteProcedureCalls {
		// First check the global localEnvironmentsRouter map for scenarios where we're
		// potentially trying to communicate between multiple different in-memory
		// instances of Environment.
		localEnvironmentsRouterLock.RLock()
		localEnv, ok := localEnvironmentsRouter[ref.Address()]
		localEnvironmentsRouterLock.RUnlock()
		if ok {
			return localEnv.InvokeActorDirectStream(
				ctx, versionStamp, ref.ServerID(), ref.ServerVersion(), ref,
				operation, payload, create)
		}

		// Separately, if the registry returned an address that looks like localhost for any
		// reason, then just route the request to the current node / this instance of
		// Environment. This prevents unnecessary RPCs when the caller happened to send a
		// request to the NOLA node that should actually run the actor invocation.
		//
		// More importantly, it also dramatically simplifies writing applications that embed
		// NOLA as a library. This helps enable the ability to write application code that
		// runs/behaves exactly the same way and follows the same code paths in production
		// and in tests.
		//
		// For example, applications leveraging the DNS-backed registry implementation can
		// use the exact same code in production and in single-node environments/tests by
		// passing "localhost" as the hostname to the DNSRegistry which will cause it to
		// always return dnsregistry.Localhost as the address for all actor references and
		// thus ensure that tests can be written without having to also ensure that a NOLA
		// server is running on the appropriate port, among other things.
		if ref.Address() == Localhost || ref.Address() == dnsregistry.Localhost {
			return localEnv.InvokeActorDirectStream(
				ctx, versionStamp, ref.ServerID(), ref.ServerVersion(), ref,
				operation, payload, create)
		}
	}

	return r.client.InvokeActorRemote(ctx, versionStamp, ref, operation, payload, create)
}

func (r *environment) freezeHeartbeatState() {
	r.heartbeatState.Lock()
	r.heartbeatState.frozen = true
	r.heartbeatState.Unlock()
}

func (r *environment) pauseHeartbeat() {
	r.heartbeatState.Lock()
	r.heartbeatState.paused = true
	r.heartbeatState.Unlock()
}

func (r *environment) resumeHeartbeat() {
	r.heartbeatState.Lock()
	r.heartbeatState.paused = false
	r.heartbeatState.Unlock()
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
