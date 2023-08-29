package virtual

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/registry/dnsregistry"
	"github.com/richardartoul/nola/virtual/types"

	"golang.org/x/exp/slog"
)

const (
	Localhost = "127.0.0.1"

	maxNumActivationsToCache = 1e6 // 1 Million.
	heartbeatTimeout         = registry.HeartbeatTTL
)

var (
	// ErrEnvironmentClosed is an error that indicates the environment is closed.
	// It can be returned when attempting to perform an operation on a closed environment.
	ErrEnvironmentClosed = errors.New("environment is closed")

	// Var so can be modified by tests.
	defaultActivationsCacheTTL                    = heartbeatTimeout
	DefaultGCActorsAfterDurationWithNoInvocations = time.Minute
)

type environment struct {
	log *slog.Logger

	// State.
	activations      *activations // Internally synchronized.
	activationsCache *activationsCache
	lastHearbeatLog  time.Time

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
	// shutdownState holds the state needed to gracefully shutdown the service.
	shutdownState struct {
		mu sync.RWMutex
		// Flag to prevent public methods from processing new requests.
		closed bool
		// inflight is a WaitGroup used to keep track of in-flight requests and wait for them to finish.
		inflight sync.WaitGroup
	}

	// Dependencies.
	serverID string
	address  string
	registry registry.Registry
	client   RemoteClient
	opts     EnvironmentOptions

	randState struct {
		sync.Mutex
		rng *rand.Rand
	}
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
	// ActivationBlacklistCacheTTL is the TTL of the activations blacklist cache.
	ActivationBlacklistCacheTTL time.Duration
	// DisableActivationCache disables the activation cache.
	DisableActivationCache bool
	// Discovery contains the discovery options.
	Discovery DiscoveryOptions
	// ForceRemoteProcedureCalls forces the environment to *always* invoke
	// actors via RPC even if the actor is activated on the node
	// that originally received the request.
	ForceRemoteProcedureCalls bool

	// CustomHostFns contains a set of additional user-defined host
	// functions that can be exposed to activated actors. This allows
	// developeres leveraging NOLA as a library to extend the environment
	// with additional host functionality.
	CustomHostFns map[string]func([]byte) ([]byte, error)

	// GCActorsAfterDurationWithNoInvocations is the duration after which an
	// activated actor that receives no invocations will be GC'd out of memory.
	//
	// The actor's shutdown function will be invoked before the actor is GC'd.
	//
	// A value of 0 will be ignored and replaced with the default value of
	// DefaultGCActorsAfterDurationWithNoInvocations. To disable this
	// functionality entirely, just use a really large value.
	GCActorsAfterDurationWithNoInvocations time.Duration

	// MaxNumShutdownWorkers specifies the number of workers used for shutting down the active actors
	// in the environment. This determines the level of parallelism and CPU resources utilized
	// during the shutdown process. By default, all available CPUs (runtime.NumCPU()) are used.
	MaxNumShutdownWorkers int

	// Logger is a logging instance used for logging messages.
	// If no logger is provided, the default logger from the slog package (slog.Default()) will be used.
	Logger *slog.Logger
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
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	if err := opts.Validate(); err != nil {
		return nil, nil, fmt.Errorf("error validating EnvironmentOptions: %w", err)
	}

	reg, err := dnsregistry.NewDNSRegistry(host, port, dnsregistry.DNSRegistryOptions{Logger: opts.Logger})
	if err != nil {
		return nil, nil, fmt.Errorf("error creating DNS registry: %w", err)
	}

	env, err := NewEnvironment(
		ctx, dnsregistry.DNSServerID, reg, registry.NewNoopModuleStore(), NewHTTPClient(), opts)
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
	// AllowFailedInitialHeartbeat can be set to true to allow the environment
	// to instantiate itself even if the initial heartbeat fails. This is useful
	// to avoid circular startup dependencies when using the leaderregistry
	// implementation which requires at least one environment to be up and running
	// to bootstrap the cluster.
	AllowFailedInitialHeartbeat bool
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

	if e.GCActorsAfterDurationWithNoInvocations < 0 {
		return fmt.Errorf("GCActorsAfterDurationWithNoInvocations must be >= 0")
	}

	return nil
}

// NewEnvironment creates a new Environment.
func NewEnvironment(
	ctx context.Context,
	serverID string,
	reg registry.Registry,
	moduleStore registry.ModuleStore,
	client RemoteClient,
	opts EnvironmentOptions,
) (Environment, error) {
	if opts.ActivationCacheTTL == 0 {
		opts.ActivationCacheTTL = defaultActivationsCacheTTL
	}
	if opts.GCActorsAfterDurationWithNoInvocations == 0 {
		opts.GCActorsAfterDurationWithNoInvocations = DefaultGCActorsAfterDurationWithNoInvocations
	}
	if opts.MaxNumShutdownWorkers == 0 {
		opts.MaxNumShutdownWorkers = runtime.NumCPU()
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("error validating EnvironmentOptions: %w", err)
	}

	if client == nil {
		client = newNOOPRemoteClient()
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

	opts.Logger = opts.Logger.With(
		slog.String("server_id", serverID),
		slog.String("address", address),
	)

	env := &environment{
		log: opts.Logger.With(
			slog.String("module", "environment"),
			slog.String("sub_service", "environment")),
		activationsCache: newActivationsCache(
			reg,
			opts.ActivationCacheTTL,
			opts.DisableActivationCache,
			opts.Logger.With(
				slog.String("module", "environment"),
				slog.String("sub_service", "activations_cache"),
			)),
		closeCh:  make(chan struct{}),
		closedCh: make(chan struct{}),
		registry: reg,
		client:   client,
		address:  address,
		serverID: serverID,
		opts:     opts,
	}
	env.randState.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	activations := newActivations(
		opts.Logger, reg, moduleStore, env, env.opts.CustomHostFns, activationsOptions{
			gcActorsAfter:               opts.GCActorsAfterDurationWithNoInvocations,
			activationBlacklistCacheTTL: opts.ActivationBlacklistCacheTTL,
		})
	env.activations = activations

	// Skip confusing log if dnsregistry is being used since it doesn't use the registry-based
	// registration mechanism in the traditional way.
	if serverID != dnsregistry.DNSServerID {
		opts.Logger.Info("registering self with address", slog.String("address", address))
	}

	// Do one heartbeat right off the bat so the environment is immediately useable.
	err := env.Heartbeat()
	if err != nil {
		if opts.Discovery.AllowFailedInitialHeartbeat {
			opts.Logger.Error(
				"failed to perform initial heartbeat, proceeding because AllowFailedInitialHeartbeat is set to true",
				slog.String("error", err.Error()))
		} else {
			return nil, fmt.Errorf("failed to perform initial heartbeat: %w, failing because AllowedFailedInitialHeartbeat is set to false", err)
		}
	}
	opts.Logger.Info("performed initial heartbeat", slog.String("address", address))

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
				if env.isHeartbeatPaused() {
					return
				}
				if err := env.Heartbeat(); err != nil {
					opts.Logger.Error("error performing background heartbeat", slog.Any("error", err))
				}
			case <-env.closeCh:
				opts.Logger.Info("shutting down environment", slog.String("serverID", env.serverID), slog.String("address", env.address))
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
	if r.isClosed() {
		return ErrEnvironmentClosed
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
	if r.isClosed() {
		return nil, ErrEnvironmentClosed
	}

	reader, err := r.InvokeActorStream(
		ctx, namespace, actorID, moduleID, operation, payload, create)
	if err != nil {
		return nil, fmt.Errorf("InvokeActor: error invoking actor: %w", err)
	}

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading actor response from stream: %w", err)
	}

	return b, nil
}

func (r *environment) InvokeActorJSON(
	ctx context.Context,
	namespace string,
	actorID string,
	moduleID string,
	operation string,
	payload any,
	create types.CreateIfNotExist,
	resp any,
) error {
	marshaled, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf(
			"error marshaling JSON payload of type: %T, err: %w", payload, err)
	}

	respBytes, err := r.InvokeActor(
		ctx, namespace, actorID, moduleID, operation, marshaled, create)
	if err != nil {
		return err
	}

	if resp != nil {
		if err := json.Unmarshal(respBytes, resp); err != nil {
			return fmt.Errorf(
				"error JSON unmarshaling response bytes into object of type: %T, err: %w",
				resp, err)
		}
	}

	return nil
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
	if err := create.Validate(); err != nil {
		return nil, fmt.Errorf("error validating CreateIfNotExist: %w", err)
	}

	resp, err := r.invokeActorStreamHelper(
		ctx, namespace, actorID, moduleID, operation, payload, create, nil)
	if err == nil {
		return resp, nil
	}

	if IsBlacklistedActivationError(err) {
		// If we received an error because the target server has blacklisted activations
		// of this actor, then we'll invalidate our cache to force the subsequent call
		// to lookup the actor's new activation location in the registry. We'll also set
		// a flag on the call to the registry to indicate that the server has blacklisted
		// that actor to ensure we get an activation on a different serer since the registry
		// may not know about the blacklist yet.
		r.activationsCache.delete(namespace, moduleID, actorID)
		blacklistedServerIDs := err.(BlacklistedActivationErr).ServerIDs()

		r.log.Warn(
			"encountered blacklisted actor, forcing activation cache refresh and retrying",
			slog.String("actor_id", fmt.Sprintf("%s::%s::%s", namespace, moduleID, actorID)),
			slog.Any("blacklisted_server_ids", blacklistedServerIDs))

		return r.invokeActorStreamHelper(
			ctx, namespace, actorID, moduleID, operation, payload, create, blacklistedServerIDs)
	}

	return nil, err
}

func (r *environment) invokeActorStreamHelper(
	ctx context.Context,
	namespace string,
	actorID string,
	moduleID string,
	operation string,
	payload []byte,
	create types.CreateIfNotExist,
	blacklistedServerIDs []string,
) (io.ReadCloser, error) {
	if r.isClosed() {
		return nil, ErrEnvironmentClosed
	}

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

	references, err := r.activationsCache.ensureActivation(
		ctx, namespace, moduleID, actorID, create.Options.ExtraReplicas, blacklistedServerIDs)
	if err != nil {
		return nil, fmt.Errorf("error ensuring actor activation: %w", err)
	}
	if len(references) == 0 {
		return nil, fmt.Errorf(
			"ensureActivation() success with 0 references for actor ID: %s", actorID)
	}

	var (
		retryPolicy   = create.Options.RetryPolicy // Retry policy for controlling the retry behavior.
		invokeCtx     = ctx                        // Context used for invoking the actor.
		cc            = func() {}                  // Function for canceling the invocation context.
		invocationErr error                        // Error encountered during the last invocation attempt.
	)
	// Perform the retry mechanism for invoking the actor using replicas.
	for retryAttempt := uint(0); retryAttempt < 1+retryPolicy.MaxNumRetries; retryAttempt++ {
		if len(references) < 1 {
			// If there are no more replicas left, the invocation is considered failed.
			// Return an error indicating the failure, including the number of attempts
			// and the last encountered error.
			return nil, fmt.Errorf(
				"failed invocation because there are no more replicas left, after %d attempts, and with last error %w",
				retryAttempt, invocationErr)
		}

		// Create a new context with a timeout for each invocation attempt.
		if retryPolicy.PerAttemptTimeout > 0 {
			invokeCtx, cc = context.WithTimeout(ctx, retryPolicy.PerAttemptTimeout)
		}
		resp, selectedReferences, err := r.invokeReferences(invokeCtx, vs, references, operation, payload, create)
		if err != nil {
			// If there was an error we can cancel the per-attempt context immediately.
			cc()
		} else {
			// However, if there was no error then we need to ensure that the cc() function
			// will eventually be called, but not until the caller is done reading the stream.
			resp = newCtxReaderCloser(cc, resp)
		}

		// If there is no error or the error is due to server blacklisting, consider the invocation successful.
		// Return the response and error to exit the retry loop.
		if err == nil || IsBlacklistedActivationError(err) {
			return resp, err
		}

		// Store the error encountered during the current invocation attempt.
		invocationErr = err

		// Remove the servers that failed to invoke the actor from the available references.
		references = filterReferences(references, selectedReferences)
	}

	// Return an error indicating that the maximum number of retries has been reached without success.
	// Include the number of attempts and the last encountered error in the error message.
	return nil, fmt.Errorf("failed invocation after maximum number of retries, after %d attempts, and with last error %w", 1+retryPolicy.MaxNumRetries, invocationErr)
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
	if r.isClosed() {
		return nil, ErrEnvironmentClosed
	}

	if err := create.Validate(); err != nil {
		return nil, fmt.Errorf("error validating CreateIfNotExist: %w", err)
	}

	reader, err := r.InvokeActorDirectStream(
		ctx, versionStamp, serverID, serverVersion,
		reference, operation, payload, create)
	if err != nil {
		return nil, fmt.Errorf("InvokeActorDirect: error invoking actor: %w", err)
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
	if r.isClosed() {
		return nil, ErrEnvironmentClosed
	}

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
	// This bug was identified using this model: https://github.com/richardartoul/nola/blob/master/proofs/stateright/activation-cache/README.md
	if heartbeatResult.ServerVersion != serverVersion {
		return nil, fmt.Errorf(
			"InvokeLocal: server version(%d) != server version from reference(%d)",
			heartbeatResult.ServerVersion, serverVersion)
	}

	return r.activations.invoke(ctx, reference, operation, create.InstantiatePayload, payload, false)
}

func (r *environment) InvokeWorker(
	ctx context.Context,
	namespace string,
	moduleID string,
	operation string,
	payload []byte,
	create types.CreateIfNotExist,
) ([]byte, error) {
	if r.isClosed() {
		return nil, ErrEnvironmentClosed
	}

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
	if r.isClosed() {
		return nil, ErrEnvironmentClosed
	}

	// TODO: The implementation of this function is nice because it just reusees a bunch of the
	//       actor logic. However, it's also less performant than it could be because it still
	//       effectively makes worker execution single-threaded perserver. We should add the
	//       ability for multiple workers of the same module ID to execute in parallel on a
	//       single server. This should be relatively straightforward to do with a few modications
	//       to activations.go.
	ref, err := types.NewVirtualWorkerReference(namespace, moduleID, moduleID)
	if err != nil {
		return nil, fmt.Errorf("InvokeWorker: error creating actor reference: %w", err)
	}

	// Workers provide none of the consistency / linearizability guarantees that actor's do, so we
	// can bypass the registry entirely and just immediately invoke the function.
	return r.activations.invoke(ctx, ref, operation, create.InstantiatePayload, payload, false)
}

func (r *environment) Close(ctx context.Context) error {
	if r.isClosed() {
		return ErrEnvironmentClosed
	}

	localEnvironmentsRouterLock.Lock()
	delete(localEnvironmentsRouter, r.address)
	localEnvironmentsRouterLock.Unlock()

	close(r.closeCh)

	// Wait for the background heartbeating mechanism to shutdown first.
	// This gives the heartbeating / discovery system time to shutdown and proactively unregister
	// itself (proactive unregister is not currently implemented, but could easily be added in the future).
	select {
	case <-r.closedCh:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Now that the heartbeating / discovery system has performed a clean shutdown, we can safely
	// begin rejecting all new requests.
	r.shutdownState.mu.Lock()
	r.shutdownState.closed = true
	r.shutdownState.mu.Unlock()

	// Now that we're no longer accepting new requests, lets wait for all outstanding
	// requests to complete before we begin shutting down the actors to avoid disrupting
	// the inflight requests.
	r.log.Info("Waiting for inflight methods to terminate...")
	start := time.Now()
	r.shutdownState.inflight.Wait()
	r.log.Info("Finished waiting for inflight methods to terminate", slog.Duration("duration", time.Since(start)))

	// Finally, we can now safely shutdown all the in-memory actors giving them the
	// opportunity to perform clean shutdown by invoking their `Shutdown` methods.
	if err := r.activations.close(ctx, r.opts.MaxNumShutdownWorkers); err != nil {
		return fmt.Errorf("failed to close actors: %w", err)
	}

	return nil
}

func (r *environment) NumActivatedActors() int {
	return r.activations.numActivatedActors()
}

func (r *environment) Heartbeat() error {
	ctx, cc := context.WithTimeout(context.Background(), heartbeatTimeout)
	defer cc()

	var (
		numActors  = r.NumActivatedActors()
		usedMemory = r.activations.memUsageBytes()
	)
	result, err := r.registry.Heartbeat(ctx, r.serverID, registry.HeartbeatState{
		NumActivatedActors: numActors,
		UsedMemory:         usedMemory,
		Address:            r.address,
	})
	if err != nil {
		return fmt.Errorf("error heartbeating: %w", err)
	}

	r.maybeLogHeartbeatState(numActors, usedMemory)

	r.heartbeatState.Lock()
	if !r.heartbeatState.frozen {
		r.heartbeatState.HeartbeatResult = result
	}
	r.heartbeatState.Unlock()

	// Ensure the latest ServerVersion is set on the activation struct as well since
	// it used to do things like construct NewBlacklistedActivationErrors, etc.
	r.activations.setServerState(r.serverID, result.ServerVersion)

	if result.MemoryBytesToShed > 0 {
		// Server told us we're using too much memory relative to our peers and
		// should shed some of our actors to force rebalancing.
		r.log.Info(
			"attempting to shed memory usage",
			slog.Int64("memory_bytes_to_shed", r.heartbeatState.MemoryBytesToShed))
		r.activations.shedMemUsage(int(r.heartbeatState.MemoryBytesToShed))
	}

	return nil
}

func (r *environment) maybeLogHeartbeatState(
	numActors int,
	usedMemory int,
) {
	// Normally, this synchronization is not required because this function is
	// called in a single-threaded loop. However, we have added additional synchronization
	// here because some integration tests force manual heartbeats to occur, which can introduce concurrent access.
	r.heartbeatState.Lock()
	if time.Since(r.lastHearbeatLog) < 10*time.Second {
		r.heartbeatState.Unlock()
		return
	}
	r.lastHearbeatLog = time.Now()
	r.heartbeatState.Unlock()

	attrs := make([]slog.Attr, 0, 8)
	attrs = append(attrs, slog.Int("num_actors", numActors))
	attrs = append(attrs, slog.Int("used_memory", usedMemory))

	var (
		topByMem       = r.activations.topNByMem(10)
		filtered       []string
		thresholdBytes = 1 << 28
	)
	for _, a := range topByMem {
		if a.memoryBytes > thresholdBytes {
			filtered = append(filtered, fmt.Sprintf("%s(%d MiB)", a.id.String(), a.memoryBytes/1024/1024))
		}
	}

	if len(filtered) > 0 {
		attrs = append(attrs, slog.Int("logging_threshold_mib", thresholdBytes/1024/1024), slog.Any("actors", filtered))
	}

	r.log.LogAttrs(
		context.Background(), slog.LevelInfo, "heartbeat state", attrs...)
}

// TODO: This is kind of a giant hack, but it's really only used for testing. The idea is that
// even when we're using local references, we still want to be able to create multiple
// environments in memory that can all "route" to each other. To accomplish this, everytime an
// environment is created in memory we added it to this global map. Once it is closed, we remove it.
var (
	localEnvironmentsRouter     map[string]Environment = map[string]Environment{}
	localEnvironmentsRouterLock sync.RWMutex
)

// invokeReferences invokes the specified operation on the given references using the appropriate
// strategy based on the values in types.CreateIfNotExist.
//
// The method returns the result/error, and a slice of the references that were used to perform
// the actual invocation so that the caller can remove them from subsequent retries, if necessary.
func (r *environment) invokeReferences(
	ctx context.Context,
	versionStamp int64,
	references []types.ActorReference,
	operation string,
	payload []byte,
	create types.CreateIfNotExist,
) (io.ReadCloser, []types.ActorReference, error) {
	// Usually references is a slice of size 1, but it may contain more than one
	// entry in the case that ReplicaSelectionStrategyBroadcast is being used.
	//
	// Intentionally shadow references here to prevent use of the wrong value
	// by accident in subsequent code.
	references, err := r.pickServerForInvocation(references, create)
	if err != nil {
		return nil, nil, fmt.Errorf("error picking server for activation: %w", err)
	}
	if len(references) == 0 {
		// This shouldn't happen since pickServerForInvocation should return
		// an error in that case.
		return nil, nil, fmt.Errorf("[invariant violated] no references available")
	}

	if len(references) == 1 {
		result, err := r.invokeSingleReference(
			ctx, versionStamp, references[0], operation, payload, create)
		return result, references, err
	}

	// In the case of a broadcast we issue all the requests in parallel and then return
	// the first successfult response. If all the responses are errors, then we return
	// all of them.

	if create.Options.RetryPolicy.PerAttemptTimeout <= 0 {
		return nil, nil, fmt.Errorf("[invariant violated] RetryPolicy.PerAttemptTimeout must be > 0 when using broadcast")
	}

	// Create a new dedicated context because in the case of a broadcast we'll return the
	// first successful response, but we don't want to cancel the request to all the other
	// replicas that have not completed yet either which may happen if we use the same parent
	// context which could be terminated once the successful response is returned.
	//
	// cc will be invoked only when all the async broadcast requests have completed which
	// is managed by the async goroutine below that drains the results channel.
	ctx, cc := context.WithTimeout(ctx, create.Options.RetryPolicy.PerAttemptTimeout)

	type respOrErr struct {
		resp io.ReadCloser
		err  error
	}
	results := make(chan respOrErr, len(references))
	for _, ref := range references {
		ref := ref // Capture for async goroutine.
		go func() {
			resp, err := r.invokeSingleReference(ctx, versionStamp, ref, operation, payload, create)
			results <- respOrErr{
				resp: resp,
				err:  err,
			}
		}()
	}

	finalResult := make(chan respOrErr)
	go func() {
		var (
			i                  = 0
			hasSentFinalResult = false
			errs               []error
		)
		for result := range results {
			if !hasSentFinalResult {
				if result.err == nil {
					hasSentFinalResult = true
					finalResult <- result
				} else {
					errs = append(errs, result.err)
				}
			} else {
				// Make sure we clean up all the resources for successful responses that
				// occur after we've already returned the first successful response.
				if result.err == nil {
					result.resp.Close()
				}
			}

			if i == len(references)-1 {
				if !hasSentFinalResult {
					finalResult <- respOrErr{
						err: errors.Join(errs...),
					}
				}
				cc()
				return
			}

			i++
		}
	}()

	select {
	case <-ctx.Done():
		return nil, references, ctx.Err()
	case final := <-finalResult:
		return final.resp, references, final.err
	}
}

func (r *environment) invokeSingleReference(
	ctx context.Context,
	versionStamp int64,
	ref types.ActorReference,
	operation string,
	payload []byte,
	create types.CreateIfNotExist,
) (io.ReadCloser, error) {
	if r.opts.ForceRemoteProcedureCalls {
		resp, err := r.client.InvokeActorRemote(ctx, versionStamp, ref, operation, payload, create)
		return resp, err
	}

	// First check the global localEnvironmentsRouter map for scenarios where we're
	// potentially trying to communicate between multiple different in-memory
	// instances of Environment.
	localEnvironmentsRouterLock.RLock()
	localEnv, ok := localEnvironmentsRouter[ref.Physical.ServerState.Address]
	localEnvironmentsRouterLock.RUnlock()
	if ok {
		resp, err := localEnv.InvokeActorDirectStream(
			ctx, versionStamp, ref.Physical.ServerID, ref.Physical.ServerVersion, ref.Virtual,
			operation, payload, create)
		return resp, err
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
	if ref.Physical.ServerState.Address == Localhost || ref.Physical.ServerState.Address == dnsregistry.Localhost {
		resp, err := localEnv.InvokeActorDirectStream(
			ctx, versionStamp, ref.Physical.ServerID, ref.Physical.ServerVersion, ref.Virtual,
			operation, payload, create)
		return resp, err
	}

	// Definitely need to invoke remotely, so just do that.
	resp, err := r.client.InvokeActorRemote(ctx, versionStamp, ref, operation, payload, create)
	return resp, err
}

func (r *environment) freezeHeartbeatState() {
	r.heartbeatState.Lock()
	r.heartbeatState.frozen = true
	r.heartbeatState.Unlock()
}

func (r *environment) isHeartbeatPaused() bool {
	r.heartbeatState.Lock()
	defer r.heartbeatState.Unlock()

	return r.heartbeatState.paused
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

func (r *environment) isClosed() bool {
	r.shutdownState.mu.RLock()
	defer r.shutdownState.mu.RUnlock()

	return r.shutdownState.closed
}

// pickServerForInvocation selects a server for invocation based on the provided
// references and retry policy. It returns the selected actor reference, its index
// in the references slice, and an error if no references are available.
func (r *environment) pickServerForInvocation(
	references []types.ActorReference,
	create types.CreateIfNotExist,
) ([]types.ActorReference, error) {
	if len(references) == 0 {
		return nil, fmt.Errorf("no references available")
	}

	switch create.Options.ReplicationStrategy {
	case types.ReplicaSelectionStrategySorted:
		// Sort the references slice based on the server ID in descending order.
		// This way, the retry selection is biased towards the replica with the highest server ID over time.
		result := findMax(references, func(i, j int) bool {
			return references[i].Physical.ServerID > references[j].Physical.ServerID
		})
		return []types.ActorReference{result}, nil
	case types.ReplicaSelectionStrategyBroadcast:
		return references, nil
	case types.ReplicaSelectionStrategyRandom:
		fallthrough
	default:
		// Use a random selection strategy by generating a random index within the range of available references.
		// This evenly distributes the retry selection among the available replicas.
		r.randState.Lock()
		defer r.randState.Unlock()
		idx := r.randState.rng.Intn(len(references))
		return []types.ActorReference{references[idx]}, nil
	}
}

// findMax finds the max element in the slice based on the provided comparator function.
//
// The comparator function should return true if the element at index i is considered
// greater than the element at index j.
func findMax[T any](slice []T, comparator func(i, j int) bool) T {
	var (
		extreme T
		idx     int
	)
	for idx = 0; idx < len(slice); idx++ {
		if idx == 0 || comparator(idx, idx-1) {
			extreme = slice[idx]
		}
	}

	return extreme
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

// actorCacheKey formats namespace/module/actor into a pooled []byte to
// avoid allocating for cache checks. The caller is responsible for
// ensuring the []byte is returned to the pool once they're done and that
// they don't retain any references to it after returning it to the pool.
func actorCacheKeyUnsafePooled(
	namespace string,
	moduleID string,
	actorID string,
) (interface{}, []byte) {
	bufIface := bufPool.Get()
	cacheKey := bufIface.([]byte)[:0]
	cacheKey = formatActorCacheKey(cacheKey, namespace, moduleID, actorID)

	// Return both the interface and the []byte so the caller can return
	// the interface to the pool directly which avoids an allocation.
	return bufIface, cacheKey
}

func formatActorCacheKey(
	dst []byte,
	namespace string,
	moduleID string,
	actorID string,
) []byte {
	if cap(dst) == 0 {
		dst = make([]byte, 0, len(namespace)+len(moduleID)+len(actorID))
	}

	dst = append(dst, []byte(namespace)...)
	dst = append(dst, []byte(moduleID)...)
	dst = append(dst, []byte(actorID)...)
	return dst
}

func filterReferences(
	toFilter []types.ActorReference,
	toRemove []types.ActorReference,
) []types.ActorReference {
	filtered := make([]types.ActorReference, 0, len(toFilter)-len(toRemove))

	for _, v1 := range toFilter {
		shouldInclude := true
		for _, v2 := range toRemove {
			if v1 == v2 {
				shouldInclude = false
				break
			}
		}
		if !shouldInclude {
			continue
		}
		filtered = append(filtered, v1)
	}

	return filtered
}

// ctxReaderCloser wraps a io.ReaderCloser and a context such that they're both
// finalized together to avoid leaking any resources.
type ctxReaderCloser struct {
	ctxCC func()
	r     io.ReadCloser
}

func newCtxReaderCloser(ctxCC func(), r io.ReadCloser) io.ReadCloser {
	return &ctxReaderCloser{
		ctxCC: ctxCC,
		r:     r,
	}
}

func (c *ctxReaderCloser) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

func (c *ctxReaderCloser) Close() error {
	defer c.ctxCC()
	return c.r.Close()
}
