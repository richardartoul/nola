package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/richardartoul/nola/virtual/registry/kv"
	"github.com/richardartoul/nola/virtual/registry/tuple"
	"github.com/richardartoul/nola/virtual/types"

	"golang.org/x/exp/slog"
	"golang.org/x/sync/singleflight"
)

const (
	// HeartbeatTTL is the maximum amount of time between server heartbeats before
	// the registry will consider a server as dead.
	//
	// TODO: Should be configurable.
	HeartbeatTTL = 5 * time.Second

	// 2GiB, see KVRegistryOptions.RebalanceMemoryThreshold for more details.
	DefaultRebalanceMemoryThreshold = 1 << 31
)

var (
	errActorDoesNotExist = errors.New("actor does not exist")

	// Make sure kvRegistry implements ModuleStore as well.
	_ ModuleStore = &validator{}
)

// IsActorDoesNotExistErr returns a boolean indicating whether the error is an
// instance of (or wraps) errActorDoesNotExist.
func IsActorDoesNotExistErr(err error) bool {
	return errors.Is(err, errActorDoesNotExist)
}

type kvRegistry struct {
	versionStampBatcher singleflight.Group
	kv                  kv.Store
	opts                KVRegistryOptions
}

// KVRegistryOptions contains the options for the KVRegistry.
type KVRegistryOptions struct {
	// DisableHighConflictOperations disables operations that
	// would lead to high conflict rates when using KV stores
	// like FoundationDB. In general enabling this feature will
	// not break correctness, but it may degrade the efficiency
	// of features like balancing actors across servers.
	DisableHighConflictOperations bool

	// RebalanceMemoryThreshold is the minimum delta between the memory usage of the
	// minimum and maximum servers before the registry will begin making balancing
	// decisions based on memory usage.
	RebalanceMemoryThreshold int

	// DisableMemoryRebalancing will disable rebalancing actors based on memory
	// usage if set.
	DisableMemoryRebalancing bool

	// MinSuccessiveHeartbeatsBeforeAllowActivations is the minimum number of
	// successive heartbeats the registry must receive from any serverID before
	// it will allow EnsureActivation() calls to succeed for any actor. This is
	// used to prevent a newly instantiated registry from making actor placement
	// decisions before every server has had the opportunity to heartbeat at least
	// once. Without this setting, newly intantiated registries may temporarily
	// assign all new actor activations to a small number of servers in the brief
	// window before its received a heartbeat (and thus is aware of the existence)
	// from all the servers in the cluster.
	MinSuccessiveHeartbeatsBeforeAllowActivations int

	// Logger is a logging instance used for logging messages.
	// If no logger is provided, the default logger from the slog
	// package (slog.Default()) will be used.
	Logger *slog.Logger
}

// NewKVRegistry creates a new KV-backed registry.
func NewKVRegistry(kv kv.Store, opts KVRegistryOptions) Registry {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.RebalanceMemoryThreshold <= 0 {
		opts.RebalanceMemoryThreshold = DefaultRebalanceMemoryThreshold
	}

	return NewValidatedRegistry(&kvRegistry{
		kv:   kv,
		opts: opts,
	})
}

func (k *kvRegistry) RegisterModule(
	ctx context.Context,
	namespace,
	moduleID string,
	moduleBytes []byte,
	opts ModuleOptions,
) (RegisterModuleResult, error) {
	r, err := k.kv.Transact(func(tr kv.Transaction) (any, error) {
		_, ok, err := tr.Get(ctx, getModulePartKey(namespace, moduleID, 0))
		if err != nil {
			return nil, err
		}
		if ok {
			return RegisterModuleResult{}, fmt.Errorf(
				"error creating module: %s in namespace: %s, already exists",
				moduleID, namespace)
		}

		rm := registeredModule{
			Bytes: moduleBytes,
			Opts:  opts,
		}
		marshaled, err := json.Marshal(&rm)
		if err != nil {
			return nil, err
		}

		for i := 0; len(marshaled) > 0; i++ {
			// Maximum value size in FoundationDB is 100_000, so split anything larger
			// over multiple KV pairs.
			numBytes := 99_999
			if len(marshaled) < numBytes {
				numBytes = len(marshaled)
			}
			toWrite := marshaled[:numBytes]
			tr.Put(ctx, getModulePartKey(namespace, moduleID, i), toWrite)
			marshaled = marshaled[numBytes:]
		}
		return RegisterModuleResult{}, err
	})
	if err != nil {
		return RegisterModuleResult{}, fmt.Errorf("RegisterModule: error: %w", err)
	}

	return r.(RegisterModuleResult), nil
}

// GetModule gets the bytes and options associated with the provided module.
func (k *kvRegistry) GetModule(
	ctx context.Context,
	namespace,
	moduleID string,
) ([]byte, ModuleOptions, error) {
	key := getModulePrefix(namespace, moduleID)
	r, err := k.kv.Transact(func(tr kv.Transaction) (any, error) {
		var (
			moduleBytes []byte
			i           = 0
		)
		err := tr.IterPrefix(ctx, key, func(k, v []byte) error {
			moduleBytes = append(moduleBytes, v...)
			i++
			return nil
		})
		if err != nil {
			return ModuleOptions{}, err
		}
		if i == 0 {
			return ModuleOptions{}, fmt.Errorf(
				"error getting module: %s, does not exist in namespace: %s",
				moduleID, namespace)
		}

		rm := registeredModule{}
		if err := json.Unmarshal(moduleBytes, &rm); err != nil {
			return ModuleOptions{}, fmt.Errorf("error unmarshaling stored module: %w", err)
		}
		return rm, nil
	})
	if err != nil {
		return nil, ModuleOptions{}, fmt.Errorf("GetModule: error: %w", err)
	}

	result := r.(registeredModule)
	return result.Bytes, result.Opts, nil
}

func (k *kvRegistry) createActor(
	ctx context.Context,
	tr kv.Transaction,
	namespace,
	actorID,
	moduleID string,
	opts types.ActorOptions,
) (CreateActorResult, error) {
	actorKey := getActorKey(namespace, actorID, moduleID)
	_, ok, err := k.getActorBytes(ctx, tr, actorKey)
	if err != nil {
		return CreateActorResult{}, err
	}
	if ok {
		return CreateActorResult{}, fmt.Errorf(
			"error creating actor with ID: %s, already exists in namespace: %s",
			actorID, namespace)
	}

	ra := registeredActor{
		Opts:       opts,
		ModuleID:   moduleID,
		Generation: 1,
	}
	marshaled, err := json.Marshal(&ra)
	if err != nil {
		return CreateActorResult{}, err
	}

	tr.Put(ctx, actorKey, marshaled)
	return CreateActorResult{}, nil
}

func (k *kvRegistry) EnsureActivation(
	ctx context.Context,
	req EnsureActivationRequest,
) (EnsureActivationResult, error) {
	actorKey := getActorKey(req.Namespace, req.ActorID, req.ModuleID)
	references, err := k.kv.Transact(func(tr kv.Transaction) (any, error) {
		ra, ok, err := k.getActor(ctx, tr, actorKey)
		if err == nil && !ok {
			_, err := k.createActor(
				ctx, tr, req.Namespace, req.ActorID, req.ModuleID, types.ActorOptions{})
			if err != nil {
				return nil, fmt.Errorf("EnsureActivation: error creating actor: %w", err)
			}
			ra, ok, err = k.getActor(ctx, tr, actorKey)
			if err != nil {
				return nil, fmt.Errorf("EnsureActivation: error getting actor: %w", err)
			}
			if !ok {
				return nil, fmt.Errorf(
					"[invariant violated] error ensuring activation of actor with ID: %s, does not exist in namespace: %s, err: %w",
					req.ActorID, req.Namespace, errActorDoesNotExist)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("EnsureActivation: error getting actor: %w", err)
		}
		if !ok {
			// Make sure we use %w to wrap the errActorDoesNotExist so the caller can use
			// errors.Is() on it.
			return nil, fmt.Errorf(
				"[invariant violated] error ensuring activation of actor with ID: %s, does not exist in namespace: %s, err: %w",
				req.ActorID, req.Namespace, errActorDoesNotExist)
		}

		serverKey := getServerKey(ra.Activation.ServerID)
		v, ok, err := tr.Get(ctx, serverKey)
		if err != nil {
			return nil, err
		}

		var (
			server       serverState
			serverExists bool
		)
		if ok {
			if err := json.Unmarshal(v, &server); err != nil {
				return nil, fmt.Errorf("error unmarsaling server state with ID: %s", req.ActorID)
			}
			serverExists = true
		}

		vs, err := tr.GetVersionStamp()
		if err != nil {
			return nil, fmt.Errorf("error getting versionstamp: %w", err)
		}

		var (
			currActivation, activationExists = ra.Activation, ra.Activation.ServerID != ""
			timeSinceLastHeartbeat           = versionSince(vs, server.LastHeartbeatedAt)
			serverID                         string
			serverAddress                    string
			serverVersion                    int64
		)
		if activationExists &&
			serverExists &&
			timeSinceLastHeartbeat < HeartbeatTTL &&
			currActivation.ServerID != req.BlacklistedServerID {
			// We have an existing activation and the server is still alive, so just use that.

			// It is acceptable to look up the ServerVersion from the server discovery key directly,
			// as long as the activation is still active, it guarantees that the server's version
			// has not changed since the activation was first created.
			serverVersion = server.ServerVersion
			serverID = currActivation.ServerID
			serverAddress = server.HeartbeatState.Address
		} else {
			// We need to create a new activation because either:
			//   1. There is no activation OR
			//   2. The server the actor is currently activated on has stopped heartbeating OR
			//   3. The server the actor is currently activated on has blacklisted this actor (most
			//       likely for balancing reasons)
			liveServers, err := getLiveServers(ctx, vs, tr)
			if err != nil {
				return nil, err
			}
			if len(liveServers) == 0 {
				return nil, fmt.Errorf("0 live servers available for new activation")
			}

			maxNumHeartbeats := 0
			for _, s := range liveServers {
				if s.NumHeartbeats > maxNumHeartbeats {
					maxNumHeartbeats = s.NumHeartbeats
				}
			}
			if maxNumHeartbeats < k.opts.MinSuccessiveHeartbeatsBeforeAllowActivations {
				return nil, fmt.Errorf(
					"maxNumHeartbeats: %d < MinSuccessiveHeartbeatsBeforeAllowActivations(%d)",
					maxNumHeartbeats, k.opts.MinSuccessiveHeartbeatsBeforeAllowActivations)
			}

			// TODO: Update this code once we support configurable replication.
			var cachedServerID string
			if len(req.CachedActivationServerIDs) > 0 {
				cachedServerID = req.CachedActivationServerIDs[0]
			}

			selected := pickServerForActivation(
				liveServers, k.opts, req.BlacklistedServerID, cachedServerID, !activationExists)
			serverID = selected.ServerID
			serverAddress = selected.HeartbeatState.Address
			serverVersion = selected.ServerVersion
			currActivation = newActivation(serverID, serverVersion)

			ra.Activation = currActivation
			marshaled, err := json.Marshal(&ra)
			if err != nil {
				return nil, fmt.Errorf("error marshaling activation: %w", err)
			}

			tr.Put(ctx, actorKey, marshaled)

			if !k.opts.DisableHighConflictOperations {
				selected.HeartbeatState.NumActivatedActors++
				marshaled, err := json.Marshal(&selected)
				if err != nil {
					return nil, fmt.Errorf("error marshaling server state: %w", err)
				}

				tr.Put(ctx, getServerKey(selected.ServerID), marshaled)
			}

			k.opts.Logger.Info(
				"activated actor on server",
				slog.String("actor_id", fmt.Sprintf("%s::%s:%s", req.Namespace, req.ModuleID, req.ActorID)),
				slog.String("server_id", selected.ServerID),
				slog.String("server_address", selected.HeartbeatState.Address),
			)
		}

		ref, err := types.NewActorReference(
			serverID, serverVersion, serverAddress, req.Namespace, ra.ModuleID, req.ActorID, ra.Generation)
		if err != nil {
			return nil, fmt.Errorf("error creating new actor reference: %w", err)
		}

		return EnsureActivationResult{
			References:   []types.ActorReference{ref},
			VersionStamp: vs,
		}, nil
	})
	if err != nil {
		return EnsureActivationResult{}, fmt.Errorf("EnsureActivation: error: %w", err)
	}

	return references.(EnsureActivationResult), nil
}

func (k *kvRegistry) GetVersionStamp(
	ctx context.Context,
) (int64, error) {
	// GetVersionStamp() is in the critical path of the entire system. It is
	// called extremely frequently. Caching it directly is unsafe and could lead
	// to correctness issues. Instead, we use a singleflight.Group to debounce/batch
	// calls to the underlying storage. This has the same effect as an extremely
	// short TTL cache, but with none of the correctness issues. In effect, the
	// system calls getVersionStamp() in a *single-threaded* loop as fast as it
	// can and each GetVersionStamp() call "gloms on" to the current outstanding
	// call (or initiates the next one if none is ongoing).
	//
	// We pass "" as the key because every call is the same.
	v, err, _ := k.versionStampBatcher.Do("", func() (any, error) {
		return k.kv.Transact(func(tr kv.Transaction) (any, error) {
			return tr.GetVersionStamp()
		})
	})
	if err != nil {
		return -1, fmt.Errorf("GetVersionStamp: error: %w", err)
	}

	return v.(int64), nil
}

func (k *kvRegistry) Heartbeat(
	ctx context.Context,
	serverID string,
	heartbeatState HeartbeatState,
) (HeartbeatResult, error) {
	key := getServerKey(serverID)
	var serverVersion int64
	result, err := k.kv.Transact(func(tr kv.Transaction) (any, error) {
		// First do all the logic to update the server's heartbeat state.
		v, ok, err := tr.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("error getting server state: %w", err)
		}

		var state serverState
		if !ok {
			vs, err := tr.GetVersionStamp()
			if err != nil {
				return nil, fmt.Errorf("error getting versionstamp: %w", err)
			}
			state = newServerState(serverID, 1, heartbeatState, vs)
		} else {
			if err := json.Unmarshal(v, &state); err != nil {
				return nil, fmt.Errorf("error unmarshaling server state: %w", err)
			}
		}

		vs, err := tr.GetVersionStamp()
		if err != nil {
			return nil, fmt.Errorf("error getting versionstamp: %w", err)
		}
		timeSinceLastHeartbeat := versionSince(vs, state.LastHeartbeatedAt)
		if timeSinceLastHeartbeat >= HeartbeatTTL {
			state.ServerVersion++
		}

		serverVersion = state.ServerVersion
		state.LastHeartbeatedAt = vs
		state.HeartbeatState = heartbeatState
		state.NumHeartbeats++

		marshaled, err := json.Marshal(&state)
		if err != nil {
			return nil, fmt.Errorf("error marshaling server state: %w", err)
		}

		tr.Put(ctx, key, marshaled)

		// Next, check if we should ask the server to shed some load to force
		// rebalancing.
		//
		// TODO: The load balancing logic should probably be tested in this
		// package directly, but right now its tested in environment.go and
		// examples/leaderregistry/main_test.go

		liveServers, err := getLiveServers(ctx, vs, tr)
		if err != nil {
			return nil, fmt.Errorf("error getting live servers during heartbeat for load balancing: %w", err)
		}

		result := HeartbeatResult{
			VersionStamp: vs,
			// VersionStamp corresponds to ~ 1 million increments per second.
			HeartbeatTTL:  int64(HeartbeatTTL.Microseconds()),
			ServerVersion: serverVersion,
		}

		min, max := minMaxMemUsage(liveServers)
		delta := max.HeartbeatState.UsedMemory - min.HeartbeatState.UsedMemory
		if delta > k.opts.RebalanceMemoryThreshold && max.ServerID == serverID {
			// If the server currently heartbeating is also the server with the highest memory usage
			// and its memory usage delta vs. the server with the lowest memory usage is above the
			// threshold, ask it to shed some actors to balance memory usage.
			result.MemoryBytesToShed = int64(delta)
		}

		return result, nil
	})
	if err != nil {
		return HeartbeatResult{}, fmt.Errorf("Heartbeat: error: %w", err)
	}
	return result.(HeartbeatResult), nil
}

func (k *kvRegistry) Close(ctx context.Context) error {
	return k.kv.Close(ctx)
}

func (k *kvRegistry) UnsafeWipeAll() error {
	return k.kv.UnsafeWipeAll()
}

func (k *kvRegistry) getActorBytes(
	ctx context.Context,
	tr kv.Transaction,
	actorKey []byte,
) ([]byte, bool, error) {
	actorBytes, ok, err := tr.Get(ctx, actorKey)
	if err != nil {
		return nil, false, fmt.Errorf(
			"error getting actor bytes for key: %s", string(actorBytes))
	}
	if !ok {
		return nil, false, nil
	}
	return actorBytes, true, nil
}

func (k *kvRegistry) getActor(
	ctx context.Context,
	tr kv.Transaction,
	actorKey []byte,
) (registeredActor, bool, error) {
	actorBytes, ok, err := k.getActorBytes(ctx, tr, actorKey)
	if err != nil {
		return registeredActor{}, false, err
	}
	if !ok {
		return registeredActor{}, false, nil
	}

	var ra registeredActor
	if err := json.Unmarshal(actorBytes, &ra); err != nil {
		return registeredActor{}, false, fmt.Errorf("error unmarshaling registered actor: %w", err)
	}

	return ra, true, nil
}

func getModulePrefix(namespace, moduleID string) []byte {
	return tuple.Tuple{namespace, "modules", moduleID}.Pack()
}

func getModulePartKey(namespace, moduleID string, part int) []byte {
	return tuple.Tuple{namespace, "modules", moduleID, part}.Pack()
}

func getActorKey(namespace, actorID, moduleID string) []byte {
	return tuple.Tuple{namespace, "actors", moduleID, actorID, "state"}.Pack()
}

func getServerKey(serverID string) []byte {
	return tuple.Tuple{"servers", serverID}.Pack()
}

func getServersPrefix() []byte {
	return tuple.Tuple{"servers"}.Pack()
}

type registeredActor struct {
	Opts       types.ActorOptions
	ModuleID   string
	Generation uint64
	Activation activation
}

type registeredModule struct {
	Bytes []byte
	Opts  ModuleOptions
}

type serverState struct {
	ServerID          string
	ServerVersion     int64
	HeartbeatState    HeartbeatState
	LastHeartbeatedAt int64
	NumHeartbeats     int
}

func newServerState(
	serverID string,
	serverVersion int64,
	heartbeatState HeartbeatState,
	lastHeartbeatedAt int64,
) serverState {
	return serverState{
		ServerID:          serverID,
		ServerVersion:     serverVersion,
		HeartbeatState:    heartbeatState,
		LastHeartbeatedAt: lastHeartbeatedAt,
		NumHeartbeats:     0,
	}
}

type activation struct {
	ServerID      string
	ServerVersion int64
}

func newActivation(serverID string, serverVersion int64) activation {
	return activation{
		ServerID:      serverID,
		ServerVersion: serverVersion,
	}
}

func versionSince(curr, prev int64) time.Duration {
	since := curr - prev
	if since < 0 {
		panic(fmt.Sprintf(
			"prev: %d, curr: %d, versionstamp did not increase monotonically",
			prev, curr))
	}
	return time.Duration(since) * time.Microsecond
}

func getLiveServers(
	ctx context.Context,
	versionStamp int64,
	tr kv.Transaction,
) ([]serverState, error) {
	liveServers := []serverState{}
	err := tr.IterPrefix(ctx, getServersPrefix(), func(k, v []byte) error {
		var currServer serverState
		if err := json.Unmarshal(v, &currServer); err != nil {
			return fmt.Errorf("error unmarshaling server state: %w", err)
		}

		if versionSince(versionStamp, currServer.LastHeartbeatedAt) < HeartbeatTTL {
			liveServers = append(liveServers, currServer)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return liveServers, nil
}

// pickServerForActivation is responsible for deciding which server to activate an actor on. It
// prioritizes activating actors on the server that currently has the lowest memory usage. However,
// all else being equal, it will tiebreak by selecting the server with the lowest number of activated
// actors.
//
// TODO: Would be nice to make this function pluggable/injectable for easier testing and to make the
// system more flexible for more use cases.
func pickServerForActivation(
	available []serverState,
	opts KVRegistryOptions,
	blacklistedServerID string,
	cachedServerID string,
	isFirstTimeObservingActor bool,
) serverState {
	if len(available) == 0 {
		panic("[invariant violated] pickServerForActivation should not be called with empty slice")
	}

	// If the caller told us which server the actor was previously activated on *and* that server
	// is still alive *and* that server is not the blacklisted server *and* this is the first time
	// this registry has seen this actor before then we "trust" the cache activation and activate
	// the actor on the server the caller says it was activated on last time it asked. This helps
	// reduce churn dramatically during leader transitions by ensuring actors remain mostly sticky
	// despite the new leader having very little state to go off of. Note that for this feature to
	// work properly the MinSuccessiveHeartbeatsBeforeAllowActivations option must be set to some
	// reasonable value (3 or 4 at least).
	if cachedServerID != "" && cachedServerID != blacklistedServerID {
		for _, s := range available {
			if s.ServerID == cachedServerID {
				return s
			}
		}
	}

	sort.Slice(available, func(i, j int) bool {
		sI, sJ := available[i], available[j]
		if opts.DisableMemoryRebalancing {
			// Memory load balancing is disabled, so just look at number of activated
			// actors.
			return sI.HeartbeatState.NumActivatedActors < sJ.HeartbeatState.NumActivatedActors
		}

		// Memory load balancing is enabled so we need to look at memory usage *and*
		// number of activated actors (as a tie breaker).
		var (
			minMemUsage = sI
			maxMemUsage = sJ
		)
		if sI.HeartbeatState.UsedMemory > sJ.HeartbeatState.UsedMemory {
			minMemUsage = sJ
			maxMemUsage = sI
		}
		if maxMemUsage.HeartbeatState.UsedMemory-minMemUsage.HeartbeatState.UsedMemory > opts.RebalanceMemoryThreshold {
			return minMemUsage == sI
		}
		return sI.HeartbeatState.NumActivatedActors < sJ.HeartbeatState.NumActivatedActors
	})

	selected := available[0]
	if len(available) > 1 && selected.ServerID == blacklistedServerID {
		selected = available[1]
	}
	return available[0]
}

func minMaxMemUsage(available []serverState) (serverState, serverState) {
	if len(available) == 0 {
		panic("[invariant violated] pickServerForActivation should not be called with empty slice")
	}

	var (
		minMemUsage = available[0]
		maxMemUsage = available[0]
	)
	for _, s := range available {
		if s.HeartbeatState.UsedMemory < minMemUsage.HeartbeatState.UsedMemory {
			minMemUsage = s
		}
		if s.HeartbeatState.UsedMemory > maxMemUsage.HeartbeatState.UsedMemory {
			maxMemUsage = s
		}
	}

	return minMemUsage, maxMemUsage
}
