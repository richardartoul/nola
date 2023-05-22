package leaderregistry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/richardartoul/nola/virtual"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/registry/dnsregistry"
	"github.com/richardartoul/nola/virtual/registry/localregistry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"
)

const (
	leaderNamespace  = "leader-namespace"
	leaderActorName  = "leader-actor"
	leaderModuleName = "leader-module"
)

// LeaderProvider is the interface that must be implemented so the leader
// registry knows which node / I.P address is the current "leader". It is
// pluggable so various different leader-election solutions can be used.
type LeaderProvider interface {
	GetLeader() (registry.Address, error)
}

type leaderRegistry struct {
	// This virtual environment is how all the nodes communicate with the
	// current leader. This is a bit of a hack to avoid having to manage
	// clients/servers manually by implementing the leader as a singleton
	// virtual actor that runs on whatever host happens to be the elected
	// leader currently.
	env    virtual.Environment
	server *virtual.Server
}

// LeaderRegistry creates a new leader-backed registry. The idea with the LeaderRegistry is
// that the user provides an implementation of LeaderProvider that uses some external mechanism
// to elect one of the NOLA nodes as the "leader" at any given moment.
//
// The node that is elected as the leader then runs an "in-memory" registry implementation. While
// the user must provide a leader election implementation via the LeaderProvider interface, the
// LeaderRegistry implementation takes care of all the actual registry logic, as well as routing
// all registry requests to whichever I.P address the LeaderProvider says is the current leader.
//
// See the comments in the method body below for more details for how the implementation works
// (by leveraging NOLA recursively).
func NewLeaderRegistry(
	ctx context.Context,
	lp LeaderProvider,
	serverID string,
	envOpts virtual.EnvironmentOptions,
) (registry.Registry, error) {
	// This is a bit of a hack/shortcut, but TLDR is that the LeaderRegistry implementation needs
	// some way for each registry implementation running on each server to "route" requests to
	// the current leader. Instead of writing a bunch of boilerplate client/server code for this,
	// I "cheated" by creating a child NOLA "cluster" that connects all the LeaderRegistries. This
	// "child" NOLA cluster uses the DNSRegistry implementation to run the "local in-memory KV"
	// registry implementation as a singleton actor.
	//
	// Basically each node running the leaderregistry code is constantly updating a child NOLA
	// "cluster" running the dnsregistry with a single I.P address that corresponds to the leader
	// and therefore all registry requests will eventually get routed to whichever node the
	// LeaderProvider says is the current leader. Running the local in-memory KV registry as an actor
	// in that NOLA cluster lets us re-use all of the in-memory KV code within the distributed
	// environment.
	//
	// So... it's turtles all the way down unfortunately, but this saves a *lot* of code that would
	// have to be written to achieve a similar (or worse) effect.
	resolver := newLeaderProviderToDNSResolver(lp)
	reg, err := dnsregistry.NewDNSRegistryFromResolver(
		resolver, "", dnsregistry.DNSRegistryOptions{})
	if err != nil {
		return nil, fmt.Errorf("NewLeaderRegistry: error creating new DNS registry from resolver: %w", err)
	}

	env, err := virtual.NewEnvironment(
		ctx, serverID, reg, registry.NewNoopModuleStore(), virtual.NewHTTPClient(), envOpts)
	if err != nil {
		return nil, fmt.Errorf("NewLeaderRegistry: error creating new virtual environment: %w", err)
	}

	env.RegisterGoModule(
		types.NewNamespacedIDNoType(leaderNamespace, leaderModuleName),
		newLeaderActorModule(serverID))

	var server *virtual.Server
	if envOpts.Discovery.DiscoveryType != virtual.DiscoveryTypeLocalHost {
		// Skip this if DiscoveryTypeLocalhost because it usually means we're running in
		// a test environment where we don't actually want to physically bind ports.
		server = virtual.NewServer(registry.NewNoopModuleStore(), env)
		go func() {
			if err := server.Start(envOpts.Discovery.Port); err != nil {
				panic(err)
			}
		}()
	}

	return registry.NewValidatedRegistry(&leaderRegistry{
		env:    env,
		server: server,
	}), nil
}

func (l *leaderRegistry) EnsureActivation(
	ctx context.Context,
	req registry.EnsureActivationRequest,
) (registry.EnsureActivationResult, error) {
	var ensureActivationResponse ensureActivationResponse
	err := l.env.InvokeActorJSON(
		ctx, leaderNamespace, leaderActorName, leaderModuleName,
		"ensureActivation", &req, types.CreateIfNotExist{}, &ensureActivationResponse)
	if err != nil {
		return registry.EnsureActivationResult{}, fmt.Errorf(
			"error invoking ensureActivation on leader: %w", err)
	}

	references := make([]types.ActorReference, 0, len(ensureActivationResponse.Activations))
	for _, a := range ensureActivationResponse.Activations {
		ref, err := types.NewActorReferenceFromJSON(a)
		if err != nil {
			return registry.EnsureActivationResult{}, fmt.Errorf(
				"error unmarshaling actor reference from JSON: %w", err)
		}
		references = append(references, ref)
	}

	return registry.EnsureActivationResult{
		References:   references,
		VersionStamp: ensureActivationResponse.VersionStamp,
	}, nil
}

func (l *leaderRegistry) GetVersionStamp(
	ctx context.Context,
) (int64, error) {
	// For now we return dnsregistry.DNSVersionstamp so we can tap into the same
	// logic in environment.go that is leveraged by the dnsregistry to make the
	// versionstamp operations (GetVersionStamp() and the versionstamp returned
	// in Heartbeat()) all basicall no-ops. This is fine because the leaderregistry
	// implementation does not attempt to guarantee linearizability like the FDB
	// registry does. Note that since there *is* a leader which can provide a
	// monotonic clock, we could just return the equivalent of
	// l.env.Invoke(leaderActor, localKV.getVersionStamp()) here and use the local
	// KV's time.Since() implementation as the versionstamp. This would be completely
	// linearizable except when the leader changes which would require some extra
	// logic to be added (proably in the LeaderProvider implementation).
	//
	// For now we ignore all of that and just do what the dnsregistry does because
	// again, we don't care about linearizability for this implementation. However,
	// we should still consider providing a real monotonically increasing value here
	// because it's useful for other things, like how activations_cache.go used it
	// to "sequence" concurrent cache refreshes and make sure the cache only retains
	// registry results that are "newer" than whatever is already cached (see the
	// comments in activation_cache.go for more details). Actually... technically
	// activations_cache.go is fine right now because it uses the versionstamp from
	// the EnsureActivation() method which *is* currently leveraging the leader's
	// monotonically incrementing clock so go figure.
	return dnsregistry.DNSVersionStamp, nil
}

func (l *leaderRegistry) Heartbeat(
	ctx context.Context,
	serverID string,
	heartbeatState registry.HeartbeatState,
) (registry.HeartbeatResult, error) {
	// TODO: Right now we only route the heartbeat to the leader registry, however,
	//       it would be better if we could route the heartbeats to the leader and
	//       a few replicas (provided by the LeaderProvider) so that when the leader
	//       fails, the new node that takes over at least already knows how many
	//       servers there are to work with. However, for now we work around this
	//       issue with the MinSuccessiveHeartbeatsBeforeAllowActivations setting.
	req := heartbeatRequest{
		ServerID:       serverID,
		HeartbeatState: heartbeatState,
	}

	var heartbeatResult registry.HeartbeatResult
	err := l.env.InvokeActorJSON(
		ctx, leaderNamespace, leaderActorName, leaderModuleName,
		"heartbeat", &req, types.CreateIfNotExist{}, &heartbeatResult)
	if err != nil {
		return registry.HeartbeatResult{}, fmt.Errorf("error heartbeating leader: %w", err)
	}

	return heartbeatResult, nil
}

func (l *leaderRegistry) Close(ctx context.Context) error {
	var (
		envErr    = l.env.Close(ctx)
		serverErr error
	)
	if l.server != nil {
		serverErr = l.server.Stop(ctx)
	}
	if envErr != nil {
		return fmt.Errorf("LeaderRegistry: Close: error closing virtual environment: %w", envErr)
	}
	if serverErr != nil {
		return fmt.Errorf("LeaderRegistry: Close: error closing server: %w", serverErr)
	}

	return nil
}

func (l *leaderRegistry) UnsafeWipeAll() error {
	return errors.New("not implemented")
}

type leaderActorModule struct {
	serverID string
}

func newLeaderActorModule(serverID string) virtual.Module {
	return &leaderActorModule{
		serverID: serverID,
	}
}

func (m *leaderActorModule) Instantiate(
	ctx context.Context,
	reference types.ActorReferenceVirtual,
	payload []byte,
	host virtual.HostCapabilities,
) (virtual.Actor, error) {
	return newLeaderActor(m.serverID), nil
}

func (m *leaderActorModule) Close(ctx context.Context) error {
	return nil
}

type leaderActor struct {
	registry registry.Registry
}

func newLeaderActor(serverID string) virtual.ActorBytes {
	return &leaderActor{
		registry: localregistry.NewLocalRegistryWithOptions(
			serverID,
			registry.KVRegistryOptions{
				// Require at least one server to heartbeat three times before allowing any
				// EnsureActivation() calls to succeed so that new registries get a
				// complete view of the cluster before making any placement decisions
				// following a leader transition.
				MinSuccessiveHeartbeatsBeforeAllowActivations: 4,
			}),
	}
}

func (a *leaderActor) MemoryUsageBytes() int {
	return 0
}

func (a *leaderActor) Invoke(
	ctx context.Context,
	operation string,
	payload []byte,
) (_ []byte, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("LeaderActor: %s: err: %w", operation, err)
		}
	}()

	switch operation {
	case wapcutils.StartupOperationName:
		return nil, nil
	case wapcutils.ShutdownOperationName:
		return nil, nil
	case "ensureActivation":
		return a.handleEnsureActivation(ctx, payload)
	case "getVersionStamp":
		return nil, errors.New("getVersionStamp not implemented")
	case "heartbeat":
		return a.handleHeartbeat(ctx, payload)
	case "unsafeWipeAll":
		return nil, a.registry.UnsafeWipeAll()
	default:
		return nil, fmt.Errorf("leaderActor: unimplemented operation: %s", operation)
	}
}

func (a *leaderActor) handleEnsureActivation(
	ctx context.Context,
	payload []byte,
) ([]byte, error) {
	var req registry.EnsureActivationRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("error unmarshaling ensureActivation request: %w", err)
	}
	result, err := a.registry.EnsureActivation(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("error ensuring activation: %w", err)
	}

	activations := make([][]byte, 0, len(result.References))
	for _, a := range result.References {
		marshaled, err := json.Marshal(a)
		if err != nil {
			return nil, fmt.Errorf("error marshaling JSON for activation: %w", err)
		}
		activations = append(activations, marshaled)
	}
	marshaled, err := json.Marshal(&ensureActivationResponse{
		Activations:  activations,
		VersionStamp: result.VersionStamp,
	})
	if err != nil {
		return nil, fmt.Errorf("error marshaling ensureActivation result: %w", err)
	}

	return marshaled, nil
}

func (a *leaderActor) handleHeartbeat(
	ctx context.Context,
	payload []byte,
) ([]byte, error) {
	var req heartbeatRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("error unmarshaling heartbeat request: %w", err)
	}

	result, err := a.registry.Heartbeat(ctx, req.ServerID, req.HeartbeatState)
	if err != nil {
		return nil, fmt.Errorf("error heartbeating: %w", err)
	}

	marshaled, err := json.Marshal(&result)
	if err != nil {
		return nil, fmt.Errorf("error marshaling heartbeat result: %w", err)
	}

	return marshaled, nil
}

func (a *leaderActor) Close(ctx context.Context) error {
	return nil
}

// leaderProviderToDNSResolver makes a LeaderProvider implement the DNSResolver
// interface.
type leaderProviderToDNSResolver struct {
	lp LeaderProvider
}

func newLeaderProviderToDNSResolver(lp LeaderProvider) dnsregistry.DNSResolver {
	return &leaderProviderToDNSResolver{
		lp: lp,
	}
}

func (lp *leaderProviderToDNSResolver) LookupIP(host string) ([]registry.Address, error) {
	// Ignore the host parameter because it doesn't matter, the leader-provider will
	// already be hard-coded with how to resolve the leader.
	leader, err := lp.lp.GetLeader()
	if err != nil {
		return nil, fmt.Errorf("error resolving leader from leader provider: %w", err)
	}

	return []registry.Address{leader}, nil
}

type heartbeatRequest struct {
	ServerID       string                  `json:"server_id"`
	HeartbeatState registry.HeartbeatState `json:"heartbeat_state"`
}

type ensureActivationResponse struct {
	Activations  [][]byte
	VersionStamp int64
}
