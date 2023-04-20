package virtual

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
	"nhooyr.io/websocket"
)

type server struct {
	// Dependencies.
	registry    registry.Registry
	environment Environment

	server   *http.Server
	wsServer *http.Server
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
func (s *server) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/register-module", s.registerModule)
	mux.HandleFunc("/api/v1/invoke-actor", s.invoke)
	mux.HandleFunc("/api/v1/invoke-actor-direct", s.invokeDirect)
	mux.HandleFunc("/api/v1/invoke-worker", s.invokeWorker)

	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return s.server.ListenAndServe()
}

// Start starts the server.
func (s *server) StartWebsocket(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/rpc/json", s.wsHandler)

	s.wsServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return s.server.ListenAndServe()
}

func (s *server) Stop(ctx context.Context) error {
	log.Print("shutting down http server")
	if err := s.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shut down http server: %w", err)
	}
	log.Print("successfully shut down HTTP server")

	if s.wsServer != nil {
		log.Print("shutting down websocket http server")
		if err := s.wsServer.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shut down http server: %w", err)
		}
		log.Print("successfully shut down websocket HTTP server")
	}

	log.Print("closing environment")
	if err := s.environment.Close(ctx); err != nil {
		return fmt.Errorf("failed to close the environment: %w", err)
	}
	log.Print("successfully closed environment")

	return nil
}

type registerModuleMessage struct {
	Namespace   string `json:"namespace"`
	ModuleID    string `json:"module_id"`
	ModuleBytes []byte `json:"module_bytes"`
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

	result, err := s.handleRegisterModule(r.Context(), registerModuleMessage{Namespace: namespace, ModuleID: moduleID, ModuleBytes: moduleBytes})
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

func (s *server) handleRegisterModule(ctx context.Context, req registerModuleMessage) (registry.RegisterModuleResult, error) {
	ctx, cc := context.WithTimeout(ctx, 60*time.Second)
	defer cc()
	return s.registry.RegisterModule(ctx, req.Namespace, req.ModuleID, req.ModuleBytes, registry.ModuleOptions{})
}

type invokeActorRequest struct {
	ServerID  string `json:"server_id"`
	Namespace string `json:"namespace"`
	types.InvokeActorRequest
	// Same data as Payload (in types.InvokeActorRequest), but different field so it doesn't
	// have to be encoded as base64.
	PayloadJSON interface{} `json:"payload_json"`
}

func (s *server) invoke(w http.ResponseWriter, r *http.Request) {
	if err := ensureHijackable(w); err != nil {
		// Error already written to w.
		return
	}

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
	result, err := s.handleInvoke(r.Context(), req)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}
	defer result.Close()

	w.WriteHeader(200)
	if _, err := io.Copy(w, result); err != nil {
		// If we get any error copying the stream into the response then we
		// need to terminate the connection to ensure that the caller observes
		// an error and not a truncated response (that appears successful because
		// of the 200 status code).
		terminateConnection(w)
	}
}

func (s *server) handleInvoke(ctx context.Context, req invokeActorRequest) (io.ReadCloser, error) {
	ctx, cc := context.WithTimeout(ctx, 5*time.Second)
	defer cc()
	return s.environment.InvokeActorStream(
		ctx, req.Namespace, req.ActorID, req.ModuleID, req.Operation, req.Payload, req.CreateIfNotExist)
}

type invokeActorDirectRequest struct {
	VersionStamp     int64                  `json:"version_stamp"`
	ServerID         string                 `json:"server_id"`
	ServerVersion    int64                  `json:"server_version"`
	Namespace        string                 `json:"namespace"`
	ModuleID         string                 `json:"module_id"`
	ActorID          string                 `json:"actor_id"`
	Generation       uint64                 `json:"generation"`
	Operation        string                 `json:"operation"`
	Payload          []byte                 `json:"payload"`
	CreateIfNotExist types.CreateIfNotExist `json:"create_if_not_exist"`
}

func (s *server) invokeDirect(w http.ResponseWriter, r *http.Request) {
	if err := ensureHijackable(w); err != nil {
		// Error already written to w.
		return
	}

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
	result, err := s.handleInvokeDirect(r.Context(), req)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}
	defer result.Close()

	w.WriteHeader(200)
	if _, err := io.Copy(w, result); err != nil {
		// If we get any error copying the stream into the response then we
		// need to terminate the connection to ensure that the caller observes
		// an error and not a truncated response (that appears successful because
		// of the 200 status code).
		terminateConnection(w)
	}
}

func (s *server) handleInvokeDirect(ctx context.Context, req invokeActorDirectRequest) (io.ReadCloser, error) {
	ctx, cc := context.WithTimeout(ctx, 5*time.Second)
	defer cc()

	ref, err := types.NewVirtualActorReference(req.Namespace, req.ModuleID, req.ActorID, uint64(req.Generation))
	if err != nil {
		return nil, err
	}

	return s.environment.InvokeActorDirectStream(
		ctx, req.VersionStamp, req.ServerID, req.ServerVersion, ref,
		req.Operation, req.Payload, req.CreateIfNotExist)
}

type invokeWorkerRequest struct {
	Namespace string `json:"namespace"`
	// TODO: Allow ModuleID to be omitted if the caller provides a WASMExecutable field which contains the
	//       actual WASM program that should be executed.
	ModuleID         string                 `json:"module_id"`
	Operation        string                 `json:"operation"`
	Payload          []byte                 `json:"payload"`
	CreateIfNotExist types.CreateIfNotExist `json:"create_if_not_exist"`
}

func (s *server) invokeWorker(w http.ResponseWriter, r *http.Request) {
	if err := ensureHijackable(w); err != nil {
		// Error already written to w.
		return
	}

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
	result, err := s.handleInvokeWorker(r.Context(), req)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}
	defer result.Close()

	w.WriteHeader(200)
	if _, err := io.Copy(w, result); err != nil {
		// If we get any error copying the stream into the response then we
		// need to terminate the connection to ensure that the caller observes
		// an error and not a truncated response (that appears successful because
		// of the 200 status code).
		terminateConnection(w)
	}
}

func (s *server) handleInvokeWorker(ctx context.Context, req invokeWorkerRequest) (io.ReadCloser, error) {
	ctx, cc := context.WithTimeout(ctx, 5*time.Second)
	defer cc()

	return s.environment.InvokeWorkerStream(
		ctx, req.Namespace, req.ModuleID, req.Operation, req.Payload, req.CreateIfNotExist)
}

// ensureHijackable and terminateConnection are used in conjunction to close tcp connections
// for requests where we've started copying the response stream into the HTTP response body
// after submitting an HTTP 200 status code, but then encounter an error reading from the
// stream. In that scenario, we want to be very careful to ensure that the caller observes
// an error (by closing the TCP connection) instead of a truncated response.

func ensureHijackable(w http.ResponseWriter) error {
	_, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "webserver doesn't support hijacking", http.StatusInternalServerError)
		return errors.New("webserver doesn't support hijacking")
	}
	return nil
}

func terminateConnection(w http.ResponseWriter) {
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		// Unclear how to handle this case
		panic(fmt.Sprintf("[invariant violated] Hijack() returned error: %v", err))
	}
	conn.Close()
}

type JsonRpcResponse struct {
	VersionTag string    `json:"jsonrpc"`
	Result     any       `json:"result"`
	Error      *RpcError `json:"error"`
	ID         uint64    `json:"id"`
}

type RpcError struct {
	Code    websocket.StatusCode `json:"code"`
	Message string               `json:"message"`
}

type JsonRpcRequest struct {
	VersionTag string          `json:"jsonrpc"`
	ID         uint64          `json:"id"`
	Method     string          `json:"method"`
	Params     json.RawMessage `json:"params"`
}

type WebsocketWrapper struct {
	*websocket.Conn
}

func (wsw WebsocketWrapper) Close() error {
	return wsw.Conn.Close(0, "")
}
