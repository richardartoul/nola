package virtual

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
	"golang.org/x/exp/slog"
	"golang.org/x/sync/singleflight"
)

// activationCache is an "intelligent" cache that tries to balance caching activations
// to prevent overloading the registry and be resilient to registry failures, while
// still updating and invalidating itself in a timely manner when the registry is
// available.
type activationsCache struct {
	sync.Mutex

	// Dependencies / configuration.
	registry            registry.Registry
	idealCacheStaleness time.Duration
	logger              *slog.Logger

	// "State".
	c       *ristretto.Cache
	deduper singleflight.Group
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
	if a.c == nil {
		// Cache disabled, load directly.
		return a.ensureActivationAndUpdateCache(
			ctx, namespace, moduleID, actorID, blacklistedServerID)
	}

	var (
		bufIface any
		cacheKey []byte
	)
	bufIface, cacheKey = actorCacheKeyUnsafePooled(namespace, moduleID, actorID)
	aceI, ok := a.c.Get(cacheKey)
	bufPool.Put(bufIface)
	if !ok {
		// Cache miss, fill cache.
		return a.ensureActivationAndUpdateCache(
			ctx, namespace, moduleID, actorID, blacklistedServerID)
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
				ctx, namespace, moduleID, actorID, blacklistedServerID)
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
	actorID,

	blacklistedServerID string,
) ([]types.ActorReference, error) {
	// TODO: Check semaphore capacity here.

	// Since this method is less common (cache miss) we just allocate instead of messing
	// around with unsafe object pooling.
	cacheKey := formatActorCacheKey(nil, namespace, moduleID, actorID)

	// Include blacklistedServerID in the dedupeKey so that "force refreshes" due to a
	// server blacklist / load-shedding an actor can be initiated *after* a regular
	// refresh has already started, but *before* it has completed.
	dedupeKey := fmt.Sprintf("%s::%s", cacheKey, blacklistedServerID)
	referencesI, err, _ := a.deduper.Do(dedupeKey, func() (any, error) {
		references, err := a.registry.EnsureActivation(ctx, registry.EnsureActivationRequest{
			Namespace: namespace,
			ModuleID:  moduleID,
			ActorID:   actorID,

			BlacklistedServerID: blacklistedServerID,
		})
		if err != nil {
			return nil, fmt.Errorf(
				"error ensuring activation of actor: %s in registry: %w",
				actorID, err)
		}
		if a.c == nil {
			return references.References, nil
		}

		ace := activationCacheEntry{
			references:           references.References,
			cachedAt:             time.Now(),
			registryVersionStamp: references.VersionStamp,
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

type activationCacheEntry struct {
	references           []types.ActorReference
	cachedAt             time.Time
	registryVersionStamp int64
}
