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
	"golang.org/x/exp/slog"
)

type server struct {
	logger *slog.Logger

	// Dependencies.
	registry    registry.Registry
	environment Environment

	server   *http.Server
	wsServer *http.Server
}

// NewServer creates a new server for the actor virtual environment.
func NewServer(
	logger *slog.Logger,
	registry registry.Registry,
	environment Environment,
) *server {
	return &server{
		logger:      logger.With(slog.String("module", "server")),
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

	return s.wsServer.ListenAndServe()
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

	result, err := s.handleRegisterModule(r.Context(), types.RegisterModuleHttpRequest{Namespace: namespace, ModuleID: moduleID, ModuleBytes: moduleBytes})
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

func (s *server) handleRegisterModule(ctx context.Context, req types.RegisterModuleHttpRequest) (registry.RegisterModuleResult, error) {
	ctx, cc := context.WithTimeout(ctx, 60*time.Second)
	defer cc()
	return s.registry.RegisterModule(ctx, req.Namespace, req.ModuleID, req.ModuleBytes, registry.ModuleOptions{})
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

	var req types.InvokeActorHttpRequest
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

func (s *server) handleInvoke(ctx context.Context, req types.InvokeActorHttpRequest) (io.ReadCloser, error) {
	ctx, cc := context.WithTimeout(ctx, 5*time.Second)
	defer cc()
	return s.environment.InvokeActorStream(
		ctx, req.Namespace, req.ActorID, req.ModuleID, req.Operation, req.Payload, req.CreateIfNotExist)
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

	var req types.InvokeActorDirectHttpRequest
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

func (s *server) handleInvokeDirect(ctx context.Context, req types.InvokeActorDirectHttpRequest) (io.ReadCloser, error) {
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

	var req types.InvokeWorkerHttpRequest
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

func (s *server) handleInvokeWorker(ctx context.Context, req types.InvokeWorkerHttpRequest) (io.ReadCloser, error) {
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
