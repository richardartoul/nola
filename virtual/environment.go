package virtual

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/google/uuid"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
)

const (
	heartbeatTimeout           = registry.MaxHeartbeatDelay
	defaultActivationsCacheTTL = heartbeatTimeout
	maxNumActivationsToCache   = 1e6 // 1 Million.
)

type environment struct {
	// State.
	activations     *activations // Internally synchronized.
	activationCache *ristretto.Cache

	// Closed when the background heartbeating goroutine should be shut down.
	closeCh chan struct{}
	// Closed when the background heartbeating goroutine completes shutting down.
	closedCh chan struct{}

	// Dependencies.
	serverID string
	address  string
	registry registry.Registry
	opts     EnvironmentOptions
}

// EnvironmentOptions is the settings for the Environment.
type EnvironmentOptions struct {
	// ActivationCacheTTL is the TTL of the activation cache.
	ActivationCacheTTL time.Duration
	// DisableActivationCache disables the activation cache.
	DisableActivationCache bool
}

// NewEnvironment creates a new Environment.
func NewEnvironment(
	ctx context.Context,
	serverID string,
	reg registry.Registry,
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

	// TODO: Eventually we need to support discovering our own IP here or having this
	//       passed in.
	address := uuid.New().String()
	env := &environment{
		activationCache: activationCache,
		closeCh:         make(chan struct{}),
		closedCh:        make(chan struct{}),
		registry:        reg,
		address:         address,
		serverID:        serverID,
		opts:            opts,
	}
	activations := newActivations(reg, env)
	env.activations = activations

	// Do one heartbeat right off the bat so the environment is immediately useable.
	err = env.heartbeat()
	if err != nil {
		return nil, fmt.Errorf("failed to perform initial heartbeat: %w", err)
	}

	localEnvironmentsRouterLock.Lock()
	localEnvironmentsRouter[address] = env
	localEnvironmentsRouterLock.Unlock()

	go func() {
		defer close(env.closedCh)
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				if err := env.heartbeat(); err != nil {
					log.Printf("error heartbeating: %v\n", err)
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

func (r *environment) Invoke(
	ctx context.Context,
	namespace string,
	actorID string,
	operation string,
	payload []byte,
) ([]byte, error) {
	var (
		cacheKey    = fmt.Sprintf("%s::%s", namespace, actorID)
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
		// Set a TTL on the cache entry so that if the generation count increases
		// it will eventually get reflected in the system even if its not immediate.
		// Note that the purpose the generation count is is for code/setting upgrades
		// so it does not need to take effect immediately.
		r.activationCache.SetWithTTL(cacheKey, references, 1, r.opts.ActivationCacheTTL)
	}
	if len(references) == 0 {
		return nil, fmt.Errorf(
			"ensureActivation() success with 0 references for actor ID: %s", actorID)
	}

	vs, err := r.registry.GetVersionStamp(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting version stamp: %w", err)
	}
	return r.invokeReferences(ctx, vs, references, operation, payload)
}

func (r *environment) InvokeLocal(
	ctx context.Context,
	versionStamp int64,
	serverID string,
	reference types.ActorReference,
	operation string,
	payload []byte,
) ([]byte, error) {
	if serverID != r.serverID {
		return nil, fmt.Errorf(
			"request for serverID: %s received by server: %s, cannot fullfil",
			serverID, r.serverID)
	}

	// TODO: if versionStamp > heartbeat blah blah.

	return r.activations.invoke(ctx, reference, operation, payload)
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
	_, err := r.registry.Heartbeat(ctx, r.serverID, registry.HeartbeatState{
		NumActivatedActors: r.numActivatedActors(),
		Address:            r.address,
	})
	return err
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
	reference := references[0]
	switch reference.Type() {
	case types.ReferenceTypeLocal:
		localEnvironmentsRouterLock.RLock()
		localEnv, ok := localEnvironmentsRouter[reference.Address()]
		localEnvironmentsRouterLock.RUnlock()
		if !ok {
			return nil, fmt.Errorf(
				"unable to route invocation for server: %s at path: %s, does not exist in global routing map",
				reference.ServerID(), reference.Address())
		}
		return localEnv.InvokeLocal(ctx, versionStamp, reference.ServerID(), reference, operation, payload)
	case types.ReferenceTypeRemoteHTTP:
		fallthrough
	default:
		return nil, fmt.Errorf(
			"reference for actor: %s has unhandled type: %v", reference.ActorID(), reference.Type())
	}
}
