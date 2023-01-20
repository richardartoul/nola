package virtual

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
)

type server struct {
	// Dependencies.
	registry    registry.Registry
	environment Environment
}

// NewServer creates a new server for the actor virtual environment.
func NewServer(
	registry registry.Registry,
	environment Environment,
) *server {
	return &server{
		registry:    registry,
		environment: environment,
	}
}

// Start starts the server.
func (s *server) Start(port int) error {
	http.HandleFunc("/api/v1/register-module", s.registerModule)
	http.HandleFunc("/api/v1/create-actor", s.createActor)
	http.HandleFunc("/api/v1/invoke-actor", s.invoke)
	http.HandleFunc("/api/v1/invoke-actor-direct", s.invokeDirect)
	http.HandleFunc("/api/v1/invoke-worker", s.invokeWorker)

	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		return err
	}

	return nil
}

// This one is a bit weird because its basically a file upload with some JSON
// so we just shove the JSON into the headers cause I'm lazy to do anything
// more clever.
func (s *server) registerModule(w http.ResponseWriter, r *http.Request) {
	var (
		namespace = r.Header.Get("namespace")
		moduleID  = r.Header.Get("module_id")
	)

	moduleBytes, err := ioutil.ReadAll(io.LimitReader(r.Body, 1<<24))
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	ctx, cc := context.WithTimeout(context.Background(), 60*time.Second)
	defer cc()
	result, err := s.registry.RegisterModule(ctx, namespace, moduleID, moduleBytes, registry.ModuleOptions{})
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	marshaled, err := json.Marshal(result)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(200)
	w.Write(marshaled)
}

type createActorRequest struct {
	Namespace string `json:"namespace"`
	ActorID   string `json:"actor_id"`
	ModuleID  string `json:"module_id"`
}

func (s *server) createActor(w http.ResponseWriter, r *http.Request) {
	jsonBytes, err := ioutil.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	var req createActorRequest
	if err := json.Unmarshal(jsonBytes, &req); err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	ctx, cc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cc()
	result, err := s.registry.CreateActor(ctx, req.Namespace, req.ActorID, req.ModuleID, registry.ActorOptions{})
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	marshaled, err := json.Marshal(result)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(200)
	w.Write(marshaled)
}

type invokeActorRequest struct {
	ServerID  string `json:"server_id"`
	Namespace string `json:"namespace"`
	ActorID   string `json:"actor_id"`
	Operation string `json:"operation"`
	Payload   []byte `json:"payload"`
	// Same data as Payload, but different field so it doesn't have to be encoded
	// as base64.
	PayloadJSON interface{} `json:"payload_json"`
}

func (s *server) invoke(w http.ResponseWriter, r *http.Request) {
	jsonBytes, err := ioutil.ReadAll(io.LimitReader(r.Body, 1<<24))
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	var req invokeActorRequest
	if err := json.Unmarshal(jsonBytes, &req); err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	if len(req.Payload) == 0 && req.PayloadJSON != nil {
		marshaled, err := json.Marshal(req.PayloadJSON)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}
		req.Payload = marshaled
	}

	// TODO: This should be configurable, probably in a header with some maximum.
	ctx, cc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cc()
	result, err := s.environment.InvokeActor(ctx, req.Namespace, req.ActorID, req.Operation, req.Payload)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(200)
	w.Write(result)
}

type invokeActorDirectRequest struct {
	VersionStamp  int64  `json:"version_stamp"`
	ServerID      string `json:"server_id"`
	ServerVersion int64  `json:"server_version"`
	Namespace     string `json:"namespace"`
	ModuleID      string `json:"module_id"`
	ActorID       string `json:"actor_id"`
	Generation    uint64 `json:"generation"`
	Operation     string `json:"operation"`
	Payload       []byte `json:"payload"`
}

func (s *server) invokeDirect(w http.ResponseWriter, r *http.Request) {
	jsonBytes, err := ioutil.ReadAll(io.LimitReader(r.Body, 1<<24))
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	var req invokeActorDirectRequest
	if err := json.Unmarshal(jsonBytes, &req); err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	// TODO: This should be configurable, probably in a header with some maximum.
	ctx, cc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cc()

	ref, err := types.NewVirtualActorReference(req.Namespace, req.ModuleID, req.ActorID, uint64(req.Generation))
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	result, err := s.environment.InvokeActorDirect(ctx, req.VersionStamp, req.ServerID, req.ServerVersion, ref, req.Operation, req.Payload)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(200)
	w.Write(result)
}

type invokeWorkerRequest struct {
	Namespace string `json:"namespace"`
	// TODO: Allow ModuleID to be omitted if the caller provides a WASMExecutable field which contains the
	//       actual WASM program that should be executed.
	ModuleID  string `json:"module_id"`
	Operation string `json:"operation"`
	Payload   []byte `json:"payload"`
}

func (s *server) invokeWorker(w http.ResponseWriter, r *http.Request) {
	jsonBytes, err := ioutil.ReadAll(io.LimitReader(r.Body, 1<<24))
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	var req invokeWorkerRequest
	if err := json.Unmarshal(jsonBytes, &req); err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	// TODO: This should be configurable, probably in a header with some maximum.
	ctx, cc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cc()

	result, err := s.environment.InvokeWorker(ctx, req.Namespace, req.ModuleID, req.Operation, req.Payload)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(200)
	w.Write(result)
}
