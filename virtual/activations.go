package virtual

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/richardartoul/nola/durable/durablewazero"
	"github.com/richardartoul/nola/virtual/futures"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"

	"github.com/wapc/wapc-go/engines/wazero"
	"golang.org/x/exp/slog"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

const (
	// TODO: This should be configurable.
	// TODO: Does 1 minute make sense as a default?
	activationBlacklistCacheTTL = time.Minute
)

type activations struct {
	sync.Mutex

	log *slog.Logger

	// State.
	_modules              map[types.NamespacedID]Module
	_actors               map[types.NamespacedActorID]futures.Future[*activatedActor]
	_actorResourceTracker *actorResourceTracker
	_blacklist            *ristretto.Cache
	moduleFetchDeduper    singleflight.Group
	serverState           struct {
		sync.RWMutex
		serverID      string
		serverVersion int64
	}

	// Dependencies.
	registry      registry.Registry
	moduleStore   registry.ModuleStore
	environment   Environment
	goModules     map[types.NamespacedIDNoType]Module
	customHostFns map[string]func([]byte) ([]byte, error)
	gcActorsAfter time.Duration
}

func newActivations(
	log *slog.Logger,
	registry registry.Registry,
	moduleStore registry.ModuleStore,
	environment Environment,
	customHostFns map[string]func([]byte) ([]byte, error),
	gcActorsAfter time.Duration,
) *activations {
	if gcActorsAfter < 0 {
		panic(fmt.Sprintf("[invariant violated] illegal value for gcActorsAfter: %d", gcActorsAfter))
	}

	blacklist, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: maxNumActivationsToCache * 10, // * 10 per the docs.
		// Maximum number of entries in cache (~1million). Note that
		// technically this is a measure in bytes, but we pass a cost of 1
		// always to make it behave as a limit on number of actor IDs.
		MaxCost: maxNumActivationsToCache,
		// Recommended default.
		BufferItems: 64,
	})
	if err != nil {
		panic(fmt.Sprintf("[invariant violated] unable to construct blacklist cache: %v", err))
	}

	return &activations{
		_modules:              make(map[types.NamespacedID]Module),
		_actors:               make(map[types.NamespacedActorID]futures.Future[*activatedActor]),
		_blacklist:            blacklist,
		_actorResourceTracker: newActorResourceTracker(),

		log:           log.With(slog.String("module", "activations")),
		registry:      registry,
		moduleStore:   moduleStore,
		environment:   environment,
		goModules:     make(map[types.NamespacedIDNoType]Module),
		customHostFns: customHostFns,
		gcActorsAfter: gcActorsAfter,
	}
}

func (a *activations) registerGoModule(id types.NamespacedIDNoType, module Module) error {
	a.Lock()
	defer a.Unlock()
	if _, ok := a.goModules[id]; ok {
		return fmt.Errorf("error registering go module with ID: %v, already registered", id)
	}
	a.goModules[id] = module
	return nil
}

// invoke has a lot of manual locking and unlocking. While error prone, this is intentional
// as we need to avoid holding the lock in certain paths that may end up doing expensive
// or high latency operations. In addition, we need to ensure that the lock is not held while
// actor.o.Invoke() is called because it may run for a long time, but also to avoid deadlocks
// when one actor ends up invoking a function on another actor running in the same environment.
func (a *activations) invoke(
	ctx context.Context,
	reference types.ActorReferenceVirtual,
	operation string,
	instantiatePayload []byte,
	invokePayload []byte,
	isTimer bool,
) (io.ReadCloser, error) {
	bufIface, cacheKey := actorCacheKeyUnsafePooled(
		reference.Namespace(), reference.ModuleID().ID, reference.ActorID().ID)
	_, ok := a._blacklist.Get(cacheKey)
	// Immediately return to the pool cause we're done with it now regardless.
	bufPool.Put(bufIface)
	if ok {
		err := fmt.Errorf(
			"actor %s is blacklisted on this server", reference.ActorID())
		return nil, NewBlacklistedActivationError(err, a.serverState.serverID)
	}

	// First check if the actor is already activated.
	a.Lock()
	actorF, ok := a._actors[reference.ActorID()]
	if !ok {
		if isTimer {
			// Timers should invoke already activated actors, but not instantiate them
			// so if the actor is not already activated then we're done.
			a.Unlock()
			return nil, nil
		}

		// Actor is not activated, so we can just activate it ourselves.
		return a.invokeNotExistWithLock(
			ctx, reference, operation, instantiatePayload, invokePayload, nil)
	}

	// Actor is activated already (or in the process of being activated). Unlock and
	// wait for the future to resolve. If the actor is already activated and ready to
	// go this will resolve immediately, otherwise it will wait for the actor to be
	// properly instantiated.
	a.Unlock()
	actor, err := actorF.Wait()
	if err != nil {
		// Something went wrong instantiating the actor, just accept the error. The
		// goroutine that originally failed to instantiate the actor will take care
		// of removing the future from the map so that subsequent invocations can try
		// to re-instantiate.
		return nil, err
	}

	if isTimer && actor.reference() != reference {
		// This is not a bug, we are *intentionally* doing pointer comparison here to ensure
		// that the reference provided by the timer invocation is the exact same pointer /
		// object reference as the activated actor's internal reference. This *guarantees*
		// that a timer is "pinned" to the instance of the actor that created it and even
		// if an actor with the same exact ID and generation count is activated, then deactivated,
		// then reactivated again, a timer created by the first activation will not invoke a
		// function on the second "instance" of the actor that did not create it. This is important
		// because it ensures that actors that schedule timers then get GC'd and subsequently
		// reactivated in-memory before that timer fires will not observe timers from previous
		// activations of themselves even if those timers are still "scheduled" in the Go
		// runtime.
		//
		// TODO: Update TestScheduleSelfTimers to actually assert on this behavior since it is
		//       currently untested.
		return nil, nil
	}

	// The actor is/was activated without error, but we still need to check the generation
	// count before we're allowed to invoke it.
	if actor.reference().Generation() >= reference.Generation() {
		// The activated actor's generation count is high enough, we can just invoke now.
		return a.invokeActivatedActor(ctx, actor, operation, invokePayload)
	}

	// The activated actor's generation count is too low. We need to reinstantiate it.
	// First, we reacquire the lock since we can't do anything outside of the critical
	// loop.
	a.Lock()

	// Next, we check if the actor is still in the map (since we released and re-acquired
	// the lock, anything could have happened in the meantime).
	actorF2, ok := a._actors[reference.ActorID()]
	if !ok {
		// Actor is no longer in the map. We can just proceed with a normal activation then.
		return a.invokeNotExistWithLock(
			ctx, reference, operation, instantiatePayload, invokePayload, nil)
	}

	// Actor is still in the map. We need to know if it has changed since we last checked.
	// Pointer comparison is fine here since if it was changed then the future pointer will
	// have changed also.
	if actorF2 == actorF {
		// If it hasn't changed then we can just pretend it does not exist.
		// invokeNotExistWithLock will ensure the old activation is properly closed as
		// well.
		return a.invokeNotExistWithLock(
			ctx, reference, operation, instantiatePayload, invokePayload, actor)
	}

	// The future has changed, the generation count should be high enough now and
	// we can just ignore the old actor (whichever Goroutine increased the generation
	// count will have closed it already)
	if actor.reference().Generation() >= reference.Generation() {
		return a.invokeActivatedActor(ctx, actor, operation, invokePayload)
	}

	// Something weird happened. Just return an error and let the caller retry.
	return nil, errors.New(
		"[invariant violated] actor generation count too low after reactivation, caller should retry")
}

func (a *activations) invokeNotExistWithLock(
	ctx context.Context,
	reference types.ActorReferenceVirtual,
	operation string,
	instantiatePayload []byte,
	invokePayload []byte,
	prevActor *activatedActor,
) (io.ReadCloser, error) {
	fut := futures.New[*activatedActor]()
	a._actors[reference.ActorID()] = fut
	a.Unlock()

	// GoSync since this goroutine needs to wait anyways.
	fut.GoSync(func() (actor *activatedActor, err error) {
		if prevActor != nil {
			if err := prevActor.close(ctx); err != nil {
				a.log.Error("error closing previous instance of actor", slog.Any("actor", reference), slog.Any("error", err))
			}
			a._actorResourceTracker.track(reference.ActorID(), 0)
		}

		defer func() {
			if err != nil {
				// If resolving the future results in an error, ensure that
				// the future gets cleared from the map so that subsequent
				// invocations will try to recreate the actor instead of
				// receiving the same hard-coded over and over again.
				delete(a._actors, reference.ActorID())
			}
		}()

		module, err := a.ensureModule(ctx, reference.ModuleID())
		if err != nil {
			return nil, fmt.Errorf(
				"error ensuring module for reference: %v, err: %w",
				reference, err)
		}

		hostCapabilities := newHostCapabilities(
			a.log, a.registry, a.environment, a, a.customHostFns, reference, a.getServerState)
		iActor, err := module.Instantiate(ctx, reference, instantiatePayload, hostCapabilities)
		if err != nil {
			return nil, fmt.Errorf(
				"error instantiating actor: %s from module: %s, err: %w",
				reference.ActorID(), reference.ModuleID(), err)
		}
		if err := assertActorIface(iActor); err != nil {
			return nil, fmt.Errorf(
				"error instantiating actor: %s from module: %s, err: %w",
				reference.ActorID(), reference.ModuleID(), err)
		}

		onGc := func() {
			a.Lock()
			defer a.Unlock()

			existing, ok := a._actors[reference.ActorID()]
			if !ok {
				// Actor has already been removed from the map, nothing else to do.
				return
			}

			if existing != fut {
				// Pointer comparison indicates that while the actor is in the map, its not
				// the *same* instance that the GC function is running for, therefore we should
				// just ignore it and do nothing.
				return
			}

			// The actor is in the map and the future pointers match so we know its the same
			// instance of the actor that created this onGc function so we should remove it.
			delete(a._actors, reference.ActorID())
			a._actorResourceTracker.track(reference.ActorID(), 0)
		}

		var currMemUsage int
		currMemUsage, actor, err = newActivatedActor(
			ctx, a.log, iActor, reference, hostCapabilities, instantiatePayload, a.gcActorsAfter, onGc)
		if err != nil {
			return nil, fmt.Errorf("error activating actor: %w", err)
		}
		a._actorResourceTracker.track(reference.ActorID(), currMemUsage)

		return actor, nil
	})

	actor, err := fut.Wait()
	if err != nil {
		return nil, err
	}

	return a.invokeActivatedActor(ctx, actor, operation, invokePayload)
}

func (a *activations) invokeActivatedActor(
	ctx context.Context,
	actor *activatedActor,
	operation string,
	invokePayload []byte,
) (io.ReadCloser, error) {
	currMemUsage, stream, err := actor.invoke(ctx, operation, invokePayload, false, false)
	if err != nil {
		return nil, err
	}
	a._actorResourceTracker.track(actor.reference().ActorID(), currMemUsage)
	return stream, nil
}

func (a *activations) ensureModule(
	ctx context.Context,
	moduleID types.NamespacedID,
) (Module, error) {
	a.Lock()
	module, ok := a._modules[moduleID]
	a.Unlock()
	if ok {
		return module, nil
	}

	// Module wasn't cached already, we need to go fetch it.
	dedupeBy := fmt.Sprintf("%s-%s", moduleID.Namespace, moduleID.ID)
	moduleI, err, _ := a.moduleFetchDeduper.Do(dedupeBy, func() (any, error) {
		// Need to check map again once we get into singleflight context in case
		// the actor was instantiated since we released the lock above and entered
		// the singleflight context.
		a.Lock()
		module, ok := a._modules[moduleID]
		a.Unlock()
		if ok {
			return module, nil
		}

		// First check if its a hard-coded Go module.
		goModID := types.NewNamespacedIDNoType(moduleID.Namespace, moduleID.ID)
		goMod, ok := a.goModules[goModID]
		if ok {
			// If it is, we're pretty much done.
			module = goMod
		} else if registry.IsNoopModuleStore(a.moduleStore) {
			// Special case just to provide a nicer error message.
			return nil, fmt.Errorf(
				"module: %s is not registered as a Go module and no non-noop module store implementation is provided", moduleID)
		} else {
			// TODO: Should consider not using the context from the request here since this
			// timeout ends up being shared across multiple different requests potentially.
			moduleBytes, _, err := a.moduleStore.GetModule(ctx, moduleID.Namespace, moduleID.ID)
			if err != nil {
				return nil, fmt.Errorf(
					"error getting module bytes from registry for module: %s, err: %w",
					moduleID, err)
			}

			hostFn := newHostFnRouter(
				a.log, a.environment, a, a.customHostFns, moduleID.Namespace, moduleID.ID)
			if len(moduleBytes) > 0 {
				// WASM byte codes exists for the module so we should just use that.
				// TODO: Hard-coded for now, but we should support using different runtimes with
				//       configuration since we've already abstracted away the module/object
				//       interfaces.
				wazeroMod, err := durablewazero.NewModule(ctx, wazero.Engine(), hostFn, moduleBytes)
				if err != nil {
					return nil, fmt.Errorf(
						"error constructing module: %s from module bytes, err: %w",
						moduleID, err)
				}

				// Wrap the wazero module so it implements Module.
				module = wazeroModule{wazeroMod}
			}
		}

		// Can set unconditionally without checking if it already exists since we're in
		// the singleflight context.
		a.Lock()
		a._modules[moduleID] = module
		a.Unlock()
		return module, nil
	})
	if err != nil {
		return nil, err
	}

	return moduleI.(Module), nil
}

func (a *activations) numActivatedActors() int {
	a.Lock()
	defer a.Unlock()
	return len(a._actors)
}

func (a *activations) memUsageBytes() int {
	// No need for lock since actorResourceTracker is already synchronized internally.
	return a._actorResourceTracker.memUsageBytes()
}

func (a *activations) setServerState(
	serverID string,
	serverVersion int64,
) {
	a.serverState.Lock()
	defer a.serverState.Unlock()
	a.serverState.serverID = serverID
	a.serverState.serverVersion = serverVersion
}

func (a *activations) getServerState() (
	serverID string,
	serverVersion int64,
) {
	a.serverState.RLock()
	defer a.serverState.RUnlock()
	return a.serverState.serverID, a.serverState.serverVersion
}

// shedMemUsage instructs the activation cache to try and shed numBytes worth of memory
// usage. It does this by figuring out the TopN actors in terms of memory usage and then
// adding some (or all) of them to the activations blacklist cache. This will cause all
// subsequent invocations for those actors to fail with a special error message/code that
// will signal to the caller that they need to communicate with the registry to find a new
// activation location for the actor.
//
// In terms of actually evicting the actors from memory, the method does not currently
// implement that functionality directly. Instead it simply waits for the existing GC mechanism
// to kick in which evicts actors from memories once they haven't received any invocations
// for a period of time (which is guaranteed to happen because of the blacklist cache). However,
// this is not ideal because it indirectly links the server's ability to shed actor's that are
// using too much memory with the TTL for evicting idle actors. This is also problematic because
// it means that repeated invocations of shedMemUsage will have no effect until all the actors
// from previous invocations have been GC'd due to idleness which makes balancing slower than it
// would otherwise have to be.
//
// TODO: Fix the issue in the paragraph above by making a helper function extracted from the
// close() method to close the actors with some concurrency.
func (a *activations) shedMemUsage(numBytes int) {
	var (
		// This is counter intuitive, but when we want to shed actors to reduce memory usage we
		// start by first shedding the actors using the *lowest* amount of memory instead of the
		// *highest*.
		//
		// The reasons for this are:
		//
		//   1. Shedding low memory usage actors is less likely to result in "flapping". Imagine
		//      the server has 1000 actors on it where 999 are using very little memory and one
		//      actor is using many GiB. If we shed the highest memory usage actor, it will get
		//      reactivated shortly on another server and then perhaps soon shed from there as
		//      well resulting in a form of "ping-pong" flopping. However, if we shed the lowest
		//      memory usage actor firsts, they're much likely to "bin-pack" nicely into the other
		//      servers without flapping. This effectively results in a model where instead of
		//      trying to bin-pack high memory actors we instead try to bin-pack low memory actors
		//      and isolate high memory actors by "draining away" all other actors and in the most
		//      extreme case leaving the high memory usage actor isolated on its own dedicated
		//      server.
		//   2. Intuitively an actor that is using a lot of memory will likely take a long time to
		//      "snapshot" and subsequently "rehydrate" when it is moved to a different server. Or,
		//      another way to think about it, is that a high memory actor has accumulated more
		//      "state". A single actor can only snapshot/rehydrated single-threaded, so we can move
		//      actors and load-balance faster if we instead move a higher number of low-memory
		//      actors instead of a lower number of high-memory actors because there are significantly
		//      more opportunitie for parallelism so we can use more CPU time to keep actor migration
		//      *wall clock* time lower.
		//
		// The downside of this approach though is obviously that it requires moving/disrupting more
		// actors. However, for the current situations NOLA is used for we believe this is the right
		// trade off.
		//
		// TODO: We could consider some kind of hybrid approach where we try and move the high memory
		//       usage actors first, but only those are that are below some threshold of memory usage.
		//       For example if the threshold was 1GiB and the top actors used:
		//           [15GiB, 8GiB, 900MiB, 500MiB, 400MiB, 50MiB, 25MiB]
		//       Then we would ignore the first two actors and begin by draining the 900 and 500 MiB
		//       actors. Then if we ran out of actors to drain from the topN, we would start iterating
		//       the bottomN. I have not put a ton of thought into it, but this might work a lot better
		//       in scenarios where there are many actors and draining the bottom N results in tons of
		//       unncessary disruptions.
		actorsByMem = a._actorResourceTracker.bottomNByMemory(1000)
		toShed      = make([]actorByMem, 0, len(actorsByMem))
		remaining   = numBytes
	)

	if len(actorsByMem) <= 1 {
		// If there is only one actor left on the server, it doesn't really matter how much
		// memory its using. We're not going to make the situation any better by moving it
		// to another server so just leave it alone since at most the actor is just
		// disrupting itself.
		a.log.Info(
			"skipping shedding actors for memory usage because there are <= 1 actors",
			slog.Int("num_actors", len(actorsByMem)))
		return
	}

	for _, a := range actorsByMem {
		if remaining <= 0 {
			break
		}

		if a.memoryBytes < remaining {
			toShed = append(toShed, a)
			remaining -= a.memoryBytes
		}
	}

	if len(toShed) == len(actorsByMem) {
		// Always ensure we retain at least one actor on the server since there
		// is no scenario (with homogenous nodes at least) where it makes sense
		// to evict all the actors from the server.
		toShed = toShed[:len(toShed)-1]
	}

	for _, v := range toShed {
		key := formatActorCacheKey(nil, v.id.Namespace, v.id.Module, v.id.ID)
		a._blacklist.SetWithTTL(key, nil, 1, activationBlacklistCacheTTL)
		a.log.Info(
			"shedding actor to reduce memory usage",
			slog.String("actor_namespace", v.id.Namespace),
			slog.String("actor_module", v.id.Module),
			slog.String("actor_id", v.id.ID))
	}

	a.log.Info(
		"done shedding actors based on memory usage",
		slog.Int("num_actors", len(actorsByMem)),
		slog.Int("num_actors_shedded", len(toShed)))
}

func (a *activations) close(ctx context.Context, numWorkers int) error {
	a.log.Info("acquiring lock for closing actor activations")
	a.Lock()
	a.log.Info("acquired lock for closing actor activations")
	defer a.Unlock()

	var (
		sem      = semaphore.NewWeighted(int64(numWorkers))
		wg       sync.WaitGroup
		closed   = int64(0)
		expected = int64(len(a._actors))
	)
	for actorId, futActor := range a._actors {
		if err := sem.Acquire(ctx, 1); err != nil {
			a.log.Error("failed to acquire lock", slog.Any("error", err))
			break
		}

		wg.Add(1)
		go func(actorId types.NamespacedActorID, futActor futures.Future[*activatedActor]) {
			defer sem.Release(1)
			defer wg.Done()

			actor, err := futActor.Wait()
			if err != nil {
				a.log.Error("failed to resolve actor future during activations clean shutdown", slog.String("actor", actorId.ID), slog.Any("error", err))
				return
			}

			a._actorResourceTracker.track(actorId, 0)
			if err := actor.close(ctx); err != nil {
				a.log.Error("failed to close actor future during activations clean shutdown", slog.String("actor", actorId.ID), slog.Any("error", err))
				return
			}

			atomic.AddInt64(&closed, 1)
		}(actorId, futActor)
	}

	a.log.Info("waiting for in-memory actors to shutdown after acquiring lock")
	wg.Wait()
	a.log.Info("done waiting for in-memory actors to shutdown after acquiring lock")

	a._actors = make(map[types.NamespacedActorID]futures.Future[*activatedActor]) // delete all entries

	if expected != closed {
		return fmt.Errorf("unable to close all the actors, expected: %d - closed: %d", expected, closed)
	}

	// This helps us catch any issues with memory usage accounting by leveraging all of the
	// existing tests.
	if a._actorResourceTracker.memUsageBytes() != 0 {
		return fmt.Errorf(
			"[invariant violated] measured actor memory usage was: %d not 0 after shutdown",
			a._actorResourceTracker.memUsageBytes())
	}

	return nil
}

type activatedActor struct {
	sync.Mutex

	_log *slog.Logger

	// Don't access directly from outside this structs own method implementations,
	// use methods like invoke() and close() instead.
	_a          Actor
	_reference  types.ActorReferenceVirtual
	_host       HostCapabilities
	_closed     bool
	_lastInvoke time.Time
	_gcAfter    time.Duration
	_gcTimer    *time.Timer
}

func newActivatedActor(
	ctx context.Context,
	log *slog.Logger,
	actor Actor,
	reference types.ActorReferenceVirtual,
	host HostCapabilities,
	instantiatePayload []byte,
	gcAfter time.Duration,
	onGc func(),
) (int, *activatedActor, error) {
	a := &activatedActor{
		_log:        log.With(slog.String("module", "activatedActor")),
		_a:          actor,
		_reference:  reference,
		_host:       host,
		_lastInvoke: time.Now(),
		_gcAfter:    gcAfter,
	}

	var gcFunc func()
	gcFunc = func() {
		a.Lock()
		defer a.Unlock()

		if a._closed {
			// Actor is already closed, nothing to do.
			return
		}

		if time.Since(a._lastInvoke) > gcAfter {
			// The actor has not been invoked recently, GC it.
			err := a.closeWithLock(context.Background())
			if err != nil {
				log.Error("error closing GC'd actor", slog.Any("error", err))
			}
			onGc()
		} else {
			// Actor was invoked recently, schedule a new GC check later.
			time.AfterFunc(gcAfter, gcFunc)
		}
	}
	gcTimer := time.AfterFunc(gcAfter, gcFunc)
	a._gcTimer = gcTimer

	currMemUsage, _, err := a.invoke(ctx, wapcutils.StartupOperationName, instantiatePayload, false, false)
	if err != nil {
		a.close(ctx)
		return 0, nil, fmt.Errorf("newActivatedActor: error invoking startup function: %w", err)
	}

	return currMemUsage, a, nil
}
func (a *activatedActor) reference() types.ActorReferenceVirtual {
	return a._reference
}

func (a *activatedActor) invoke(
	ctx context.Context,
	operation string,
	payload []byte,
	alreadyLocked bool,
	isClosing bool,
) (int, io.ReadCloser, error) {
	if !alreadyLocked {
		a.Lock()
		defer a.Unlock()
	}

	if a._closed {
		return 0, nil, fmt.Errorf("tried to invoke actor: %v which has already been closed", a._reference)
	}

	// Set a._lastInvoke to now so that if the timer function runs after we release the lock it will
	// immediately see that an invocation has run recently.
	a._lastInvoke = time.Now()
	if !isClosing {
		// In addition, Reset the timer manually for the common case in which the actor has not expired
		// yet which spares the runtime the cost of spawning a goroutine to run the GC function just to
		// immediately check a._lastInvoke and see that it is recent.
		//
		// Note that we skip this branch if the actor is closed to prevent invoking the Shutdown
		// operation during close from triggering an additional GC reschedule.
		a._gcTimer.Reset(a._gcAfter)
	}

	// Workers can't have KV storage because they're not global singletons like actors
	// are. They're also not registered with the Registry explicitly, so we can skip
	// this step in that case.
	if a.reference().ActorID().IDType != types.IDTypeWorker {
		result, err := a._host.Transact(ctx, func(tr registry.ActorKVTransaction) (any, error) {
			streamActor, ok := a._a.(ActorStream)
			if ok {
				// This actor has support for the streaming interface so we should use that
				// directly since its more efficient.
				return streamActor.InvokeStream(ctx, operation, payload, tr)
			}

			// The actor doesn't support streaming responses, we'll convert the returned []byte
			// to a stream ourselves.
			return a._a.(ActorBytes).Invoke(ctx, operation, payload, tr)
		})
		if err != nil {
			return 0, nil, err
		}

		currMemUsage := a._a.MemoryUsageBytes()
		if result == nil {
			// Actor returned nil stream, convert it to an empty one.
			return currMemUsage, ioutil.NopCloser(bytes.NewReader(nil)), nil
		}

		stream, ok := result.(io.ReadCloser)
		if ok {
			return currMemUsage, stream, nil
		}
		return currMemUsage, io.NopCloser(bytes.NewBuffer(result.([]byte))), nil
	}

	// Use a noopTransaction because its a worker not an actor and workers don't get
	// access to KV storage / transactions.
	var noopTxn registry.ActorKVTransaction = noopTransaction{}
	streamActor, ok := a._a.(ActorStream)
	if ok {
		// This module has support for the streaming interface so we should use that
		// directly since its more efficient.
		stream, err := streamActor.InvokeStream(ctx, operation, payload, noopTxn)
		if err != nil {
			return 0, nil, err
		}
		return a._a.MemoryUsageBytes(), stream, nil
	}

	// The actor doesn't support streaming responses, we'll convert the returned []byte
	// to a stream ourselves.
	resp, err := a._a.(ActorBytes).Invoke(ctx, operation, payload, noopTxn)
	if err != nil {
		return 0, nil, err
	}
	return a._a.MemoryUsageBytes(), io.NopCloser(bytes.NewBuffer(resp)), nil
}

func (a *activatedActor) close(ctx context.Context) error {
	a.Lock()
	defer a.Unlock()
	return a.closeWithLock(ctx)
}

func (a *activatedActor) closeWithLock(ctx context.Context) error {
	if a._closed {
		return nil
	}

	// TODO: We should let a retry policy be specific for this before the actor is finally
	// evicted, but we just evict regardless of failure for now.
	_, _, err := a.invoke(ctx, wapcutils.ShutdownOperationName, nil, true, true)
	if err != nil {
		a._log.Error(
			"error invoking shutdown operation for actor", slog.Any("actor", a._reference), slog.Any("error", err))
	}

	a._closed = true

	return a._a.Close(ctx)
}

func assertActorIface(actor Actor) error {
	if actor == nil {
		return errors.New("module instantiated nil actor")
	}

	var (
		_, implementsByteActor   = actor.(ActorBytes)
		_, implementsStreamActor = actor.(ActorStream)
	)
	if implementsByteActor || implementsStreamActor {
		return nil
	}

	return fmt.Errorf("%T does not implement virtual.ActorBytes or virtual.ActorStream", actor)
}
