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

// LeaderRegistry creates a new leader-backed registry.
// TODO: Explain what this means. Explain options.
func NewLeaderRegistry(
	ctx context.Context,
	lp LeaderProvider,
	serverID string,
	envOpts virtual.EnvironmentOptions,
) (registry.Registry, error) {
	// TODO: Explain this.
	resolver := newLeaderProviderToDNSResolver(lp)
	reg, err := dnsregistry.NewDNSRegistryFromResolver(resolver, "", dnsregistry.DNSRegistryOptions{})
	if err != nil {
		return nil, fmt.Errorf("NewLeaderRegistry: error creating new DNS registry from resolver: %w", err)
	}

	env, err := virtual.NewEnvironment(
		ctx, serverID, reg, registry.NewNoopModuleStore(), virtual.NewHTTPClient(), envOpts)
	if err != nil {
		return nil, fmt.Errorf("NewLeaderRegistry: error creating new virtual environment: %w", err)
	}

	env.RegisterGoModule(types.NewNamespacedIDNoType(leaderNamespace, leaderModuleName), newLeaderActorModule())

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
	// TODO: DOes this make sense?
	// Must always return 1 because <= 0 is not a legal versionstamp in the system.
	return dnsregistry.DNSVersionStamp, nil
}

func (l *leaderRegistry) BeginTransaction(
	ctx context.Context,
	namespace string,
	actorID string,
	moduleID string,
	serverID string,
	serverVersion int64,
) (_ registry.ActorKVTransaction, err error) {
	return nil, errors.New("BeginTransaction not implemented")
}

func (l *leaderRegistry) Heartbeat(
	ctx context.Context,
	serverID string,
	heartbeatState registry.HeartbeatState,
) (registry.HeartbeatResult, error) {
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
}

func newLeaderActorModule() virtual.Module {
	return &leaderActorModule{}
}

func (m *leaderActorModule) Instantiate(
	ctx context.Context,
	reference types.ActorReferenceVirtual,
	payload []byte,
	host virtual.HostCapabilities,
) (virtual.Actor, error) {
	return newLeaderActor(), nil
}

func (m *leaderActorModule) Close(ctx context.Context) error {
	return nil
}

type leaderActor struct {
	registry registry.Registry
}

func newLeaderActor() virtual.ActorBytes {
	return &leaderActor{
		registry: localregistry.NewLocalRegistry(),
	}
}

func (a *leaderActor) MemoryUsageBytes() int {
	return 0
}

func (a *leaderActor) Invoke(
	ctx context.Context,
	operation string,
	payload []byte,
	transaction registry.ActorKVTransaction,
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
		// TODO: Implement me.
		return nil, errors.New("ensureActivation not implemented")
	case "beginTransaction":
		return nil, errors.New("beginTransaction not implemented")
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
		marshaled, err := a.MarshalJSON()
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
