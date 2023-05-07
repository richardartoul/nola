package virtual

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
	"golang.org/x/exp/slog"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

var (
	// TODO: Make these configurable.
	defaultMaxConcurrentEnsureActivationCalls = runtime.NumCPU() * 16
	defaultActivationCacheTimeout             = 5 * time.Second
)

// activationCache is an "intelligent" cache that tries to balance:
//  1. Caching activations to prevent overloading the registry.
//  2. Being resilient to arbitrarily long registry failures for actors whose activations are already cached.
//  3. Updating in a timely manner and invalidating itself when the registry is healthy and available.
type activationsCache struct {
	sync.Mutex

	// Dependencies / configuration.
	registry            registry.Registry
	idealCacheStaleness time.Duration
	logger              *slog.Logger

	// "State".
	ensureSem *semaphore.Weighted
	c         *ristretto.Cache
	deduper   singleflight.Group
}

func newActivationsCache(
	registry registry.Registry,
	idealCacheStaleness time.Duration,
	disableCache bool,
	logger *slog.Logger,
) *activationsCache {
	if registry == nil {
		panic("registry cannot be nil")
	}

	var (
		c   *ristretto.Cache
		err error
	)
	if !disableCache {
		c, err = ristretto.NewCache(&ristretto.Config{
			NumCounters: maxNumActivationsToCache * 10, // * 10 per the docs.
			// Maximum number of entries in cache (~1million). Note that
			// technically this is a measure in bytes, but we pass a cost of 1
			// always to make it behave as a limit on number of activations.
			MaxCost: maxNumActivationsToCache,
			// Recommended default.
			BufferItems: 64,
		})
		if err != nil {
			panic(err)
		}
	}

	return &activationsCache{
		ensureSem:           semaphore.NewWeighted(int64(defaultMaxConcurrentEnsureActivationCalls)),
		c:                   c,
		registry:            registry,
		idealCacheStaleness: idealCacheStaleness,
		logger:              logger,
	}
}

func (a *activationsCache) ensureActivation(
	ctx context.Context,
	namespace,
	moduleID,
	actorID string,

	blacklistedServerID string,
) ([]types.ActorReference, error) {
	// Ensure we have a short timeout when communicating with registry.
	ctx, cc := context.WithTimeout(ctx, defaultActivationCacheTimeout)
	defer cc()

	if a.c == nil {
		// Cache disabled, load directly.
		return a.ensureActivationAndUpdateCache(
			ctx, namespace, moduleID, actorID, nil, blacklistedServerID)
	}

	var (
		bufIface any
		cacheKey []byte
	)
	bufIface, cacheKey = actorCacheKeyUnsafePooled(namespace, moduleID, actorID)
	aceI, ok := a.c.Get(cacheKey)
	bufPool.Put(bufIface)
	// Cache miss, fill the cache.
	if !ok ||
		// There is an existing cache entry, however, it was satisfied by a request that did not provide
		// the same blacklistedServerID we have currently. We must ignore this entry because it could be
		// stale and end up routing us back to the blacklisted server ID.
		(blacklistedServerID != "" && aceI.(activationCacheEntry).blacklistedServerID != blacklistedServerID) {
		var cachedReferences []types.ActorReference
		if ok {
			cachedReferences = aceI.(activationCacheEntry).references
		}
		return a.ensureActivationAndUpdateCache(
			ctx, namespace, moduleID, actorID, cachedReferences, blacklistedServerID)
	}

	// Cache hit, return result from cache but check if we should proactively refresh
	// the cache also.

	ace := aceI.(activationCacheEntry)
	// TODO: The fact that we do this async is a bit lame because it means you'll get an error
	// for basically every actor when a server dies the first time. We could do it pre-emptively,
	// but we need a circuit breaker to guard against that resulting in high latency.
	if time.Since(ace.cachedAt) > a.idealCacheStaleness {
		ctx, cc := context.WithTimeout(context.Background(), 5*time.Second)
		go func() {
			defer cc()
			_, err := a.ensureActivationAndUpdateCache(
				ctx, namespace, moduleID, actorID, ace.references, blacklistedServerID)
			if err != nil {
				a.logger.Error(
					"error refreshing activation cache in background",
					slog.String("error", err.Error()))
			}
		}()
	}

	return ace.references, nil
}

func (a *activationsCache) delete(
	namespace,
	moduleID,
	actorID string,
) {
	bufIface, cacheKey := actorCacheKeyUnsafePooled(namespace, moduleID, actorID)
	defer bufPool.Put(bufIface)

	a.c.Del(cacheKey)
	a.deduper.Forget(string(cacheKey))
}

func (a *activationsCache) ensureActivationAndUpdateCache(
	ctx context.Context,
	namespace,
	moduleID,
	actorID string,

	cachedReferences []types.ActorReference,
	blacklistedServerID string,
) ([]types.ActorReference, error) {
	// Since this method is less common (cache miss) we just allocate instead of messing
	// around with unsafe object pooling.
	cacheKey := formatActorCacheKey(nil, namespace, moduleID, actorID)

	// Include blacklistedServerID in the dedupeKey so that "force refreshes" due to a
	// server blacklist / load-shedding an actor can be initiated *after* a regular
	// refresh has already started, but *before* it has completed.
	dedupeKey := fmt.Sprintf("%s::%s", cacheKey, blacklistedServerID)
	referencesI, err, _ := a.deduper.Do(dedupeKey, func() (any, error) {
		var cachedServerIDs []string
		for _, ref := range cachedReferences {
			cachedServerIDs = append(cachedServerIDs, ref.ServerID())
		}

		// Acquire the semaphore before making the network call to avoid DDOSing the
		// registry in pathological workloads/scenarios.
		if err := a.ensureSem.Acquire(ctx, 1); err != nil {
			return nil, fmt.Errorf(
				"context expired while waiting to acquire ensureActivation semaphore: %w",
				err)
		}
		references, err := a.registry.EnsureActivation(ctx, registry.EnsureActivationRequest{
			Namespace: namespace,
			ModuleID:  moduleID,
			ActorID:   actorID,

			BlacklistedServerID:       blacklistedServerID,
			CachedActivationServerIDs: cachedServerIDs,
		})
		// Release the semaphore as soon as we're done with the network call since the purpose
		// of this semaphore is really just to avoid DDOSing the registry.
		a.ensureSem.Release(1)
		if err != nil {
			existingAceI, ok := a.c.Get(cacheKey)
			if ok {
				// This is a bit weird, but the idea is that if the registry is down, we don't
				// want to spam it with a new refresh attempt everytime the previous one completed
				// and failed. To avoid that spam we update the cachedAt value within the
				// singleflight function so we'll wait at least idealCacheStaleness between each
				// attempt to refresh the cache.
				existingAce := existingAceI.(activationCacheEntry)
				existingAce.cachedAt = time.Now()
				a.c.Set(cacheKey, existingAce, 1)
			}
			return nil, fmt.Errorf(
				"error ensuring activation of actor: %s in registry: %w",
				actorID, err)
		}

		for _, ref := range references.References {
			if ref.ServerID() == blacklistedServerID {
				return nil, fmt.Errorf(
					"[invariant violated] registry returned blacklisted server ID: %s in references",
					blacklistedServerID)
			}
		}

		if a.c == nil {
			// Cache is disabled, just return immediately.
			return references.References, nil
		}

		ace := activationCacheEntry{
			references:           references.References,
			cachedAt:             time.Now(),
			registryVersionStamp: references.VersionStamp,
			blacklistedServerID:  blacklistedServerID,
		}

		// a.c is internally synchronized, but we use a lock here so we can do an atomic
		// compare-and-swap which the ristretto interface does not support.
		a.Lock()
		defer a.Unlock()
		existingAceI, ok := a.c.Get(cacheKey)
		if ok {
			// Make sure we always retain the cache entry with the highest registry
			// versionstamp which ensures that we never overwrite the cache with a more
			// stale result due to async non-determinism.
			existingAce := existingAceI.(activationCacheEntry)
			// Note that it is important that we allow the cache to be overwritten in the
			// case where existingAce.registryVersionStamp == ace.registryVersionStamp because
			// some registry implementations like dnsregistry (in the current implementation at
			// least) always return the exact same constant value for the versionstamp so we need
			// to ensure that the cache will still eventually update in that case.
			if existingAce.registryVersionStamp > ace.registryVersionStamp {
				return existingAce.references, nil
			}
		}
		// Otherwise, the current cache fill was initiated *after* whatever is currently cached
		// (or nothing is currently cached) therefore its safe to overwrite it.
		a.c.Set(cacheKey, ace, 1)
		return references.References, nil
	})
	if err != nil {
		return nil, err
	}

	return referencesI.([]types.ActorReference), nil
}

// activationCacheEntry is stored in the cache at a key to represent a cached actor activation.
type activationCacheEntry struct {
	references           []types.ActorReference
	cachedAt             time.Time
	registryVersionStamp int64
	blacklistedServerID  string
}
