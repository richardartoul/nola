package virtual

import (
	"context"
	"fmt"
	"runtime"
	"strings"
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

	extraReplicas uint64,
	blacklistedServerIDs []string,
) ([]types.ActorReference, error) {
	// Ensure we have a short timeout when communicating with registry.
	ctx, cc := context.WithTimeout(ctx, defaultActivationCacheTimeout)
	defer cc()

	isServerIDBlacklisted := types.StringSliceToSet(blacklistedServerIDs)

	if a.c == nil {
		// Cache disabled, load directly.
		return a.ensureActivationAndUpdateCache(
			ctx, namespace, moduleID, actorID, extraReplicas, nil, isServerIDBlacklisted, blacklistedServerIDs)
	}

	var (
		bufIface any
		cacheKey []byte
	)
	bufIface, cacheKey = actorCacheKeyUnsafePooled(namespace, moduleID, actorID)
	aceI, ok := a.c.Get(cacheKey)
	bufPool.Put(bufIface)

	var (
		cachedReferences                []types.ActorReference
		nonBlacklistedCachedReferences  []types.ActorReference
		currentBlacklistedIDsAreInvalid = false
	)

	// Check if any of the servers the current request wants to blacklist are not marked as blacklisted in the cache.
	// If any server is not blacklisted in the cache, it suggests that the cache entry might be stale and could potentially
	// route us back to the blacklisted server ID. In such cases, the cache needs to be refreshed, and the existing entry should be ignored.
	//
	// Additionally, create a new slice `nonBlacklistedCachedReferences` that only includes the references from the cache
	// that belong to non-blacklisted servers. This filtered slice will be used for subsequent processing.
	if ok {
		blacklistedIDsFromCache := aceI.(activationCacheEntry).blacklistedServerIDs
		cachedReferences = aceI.(activationCacheEntry).references

		currentBlacklistedIDsAreInvalid = len(blacklistedIDsFromCache) != len(blacklistedServerIDs)
		if !currentBlacklistedIDsAreInvalid {
			for _, id := range blacklistedIDsFromCache {
				if !isServerIDBlacklisted[id] {
					currentBlacklistedIDsAreInvalid = true
					break
				}
			}
		}

		for _, ref := range cachedReferences {
			if !isServerIDBlacklisted[ref.Physical.ServerID] {
				nonBlacklistedCachedReferences = append(nonBlacklistedCachedReferences, ref)
			}
		}
	}

	// Cache miss, not enough non-blacklisted replicas, or invalid blacklistedIDs list, then fill the cache.
	// If there is a cache entry but it was satisfied by a request with a different blacklistedServerID,
	// we must ignore the entry to avoid routing to a potentially stale blacklisted server.
	// By forcing a cache update, we prevent routing to the blacklisted server ID and ensure fresh data.
	if !ok || (1+extraReplicas) > uint64(len(nonBlacklistedCachedReferences)) ||
		// There is an existing cache entry, however, it was satisfied by a request that did not provide
		// the same blacklistedServerID we have currently. We must ignore this entry because it could be
		// stale and end up routing us back to the blacklisted server ID.
		currentBlacklistedIDsAreInvalid {
		// Force cache update and ignore the existing entry to prevent routing to blacklisted server ID.
		return a.ensureActivationAndUpdateCache(
			ctx, namespace, moduleID, actorID, extraReplicas, cachedReferences, isServerIDBlacklisted, blacklistedServerIDs)
	}

	// Cache hit, return result from cache but check if we should proactively refresh
	// the cache also.
	ace := aceI.(activationCacheEntry)
	// TODO: Jitter here.
	if time.Since(ace.cachedAt) > a.idealCacheStaleness {
		ctx, cc := context.WithTimeout(context.Background(), 5*time.Second)
		go func() {
			defer cc()
			_, err := a.ensureActivationAndUpdateCache(
				ctx, namespace, moduleID, actorID, extraReplicas, ace.references, isServerIDBlacklisted, blacklistedServerIDs)
			if err != nil {
				a.logger.Error(
					"error refreshing activation cache in background",
					slog.String("error", err.Error()))
			}
		}()
	}

	return limit(nonBlacklistedCachedReferences, 1+extraReplicas), nil
}

func limit(slice []types.ActorReference, min uint64) []types.ActorReference {
	if len(slice) > int(min) {
		return slice[:min]
	}
	return slice
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

	extraReplicas uint64,
	cachedReferences []types.ActorReference,
	isServerIDBlacklisted map[string]bool,
	blacklistedServerIDs []string,
) ([]types.ActorReference, error) {
	// Since this method is less common (cache miss) we just allocate instead of messing
	// around with unsafe object pooling.
	cacheKey := formatActorCacheKey(nil, namespace, moduleID, actorID)

	// Include blacklistedServerID in the dedupeKey so that "force refreshes" due to a
	// server blacklist / load-shedding an actor can be initiated *after* a regular
	// refresh has already started, but *before* it has completed.
	dedupeKey := fmt.Sprintf("%s::%s", cacheKey, strings.Join(blacklistedServerIDs, ","))
	referencesI, err, _ := a.deduper.Do(dedupeKey, func() (any, error) {
		var cachedServerIDs []string
		for _, ref := range cachedReferences {
			cachedServerIDs = append(cachedServerIDs, ref.Physical.ServerID)
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

			ExtraReplicas:             extraReplicas,
			BlacklistedServerIDs:      blacklistedServerIDs,
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
			if isServerIDBlacklisted[ref.Physical.ServerID] {
				return nil, fmt.Errorf(
					"[invariant violated] registry returned blacklisted server ID: %s in references",
					ref.Physical.ServerID)
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
			registryServerID:     references.RegistryServerID,
			blacklistedServerIDs: blacklistedServerIDs,
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
			//
			// Note that we can only retain the version with the highest versionstamp if the
			// registry server ID has not changed since a registry's versionstamp is only
			// guaranteed to be monotonically increasing for a given "instantiation". If the
			// registry server ID is no longer the same, then we must accept the new value to
			// avoid retaining stale values for extremely long periods of time after regsitry
			// leader transitions.
			if existingAce.registryServerID == ace.registryServerID &&
				existingAce.registryVersionStamp > ace.registryVersionStamp {
				a.logger.Warn(
					"skipping activation cache update due to new entry being more stale than existing",
					slog.String("existing_registry_server_id", existingAce.registryServerID),
					slog.String("new_registry_server_id", ace.registryServerID),
					slog.Int64("existing_registry_version_Stamp", existingAce.registryVersionStamp),
					slog.Int64("new_registry_version_stamp", ace.registryVersionStamp))
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
	registryServerID     string
	blacklistedServerIDs []string
}
