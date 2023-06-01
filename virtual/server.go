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
	"sync"
	"time"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
)

const DefaultHTTPRequestTimeout = 15 * time.Second

type Server struct {
	sync.Mutex

	// Dependencies.
	moduleStore registry.ModuleStore
	environment Environment

	server *http.Server
}

// NewServer creates a new server for the actor virtual environment.
func NewServer(
	moduleStore registry.ModuleStore,
	environment Environment,
) *Server {
	return &Server{
		moduleStore: moduleStore,
		environment: environment,
	}
}

// Start starts the server.
func (s *Server) Start(port int) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/register-module", s.registerModule)
	mux.HandleFunc("/api/v1/invoke-actor", s.invoke)
	mux.HandleFunc("/api/v1/invoke-actor-direct", s.invokeDirect)
	mux.HandleFunc("/api/v1/invoke-worker", s.invokeWorker)

	s.Lock()
	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
	s.Unlock()

	if err := s.server.ListenAndServe(); err != nil {
		return err
	}

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	log.Print("shutting down http server")
	s.Lock()
	if s.server == nil {
		return nil
	}
	if err := s.server.Shutdown(ctx); err != nil {
		s.Unlock()
		return fmt.Errorf("failed to shut down http server: %w", err)
	}
	s.Unlock()
	log.Print("successfully shut down HTTP server")

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
func (s *Server) registerModule(w http.ResponseWriter, r *http.Request) {
	var (
		namespace = r.Header.Get("namespace")
		moduleID  = r.Header.Get("module_id")
	)

	moduleBytes, err := ioutil.ReadAll(io.LimitReader(r.Body, 1<<24))
	if err != nil {
		writeStatusCodeForError(w, err)
		w.Write([]byte(err.Error()))
		return
	}

	result, err := s.moduleStore.RegisterModule(getContextFromRequest(r), namespace, moduleID, moduleBytes, registry.ModuleOptions{})
	if err != nil {
		writeStatusCodeForError(w, err)
		w.Write([]byte(err.Error()))
		return
	}

	marshaled, err := json.Marshal(result)
	if err != nil {
		writeStatusCodeForError(w, err)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(200)
	w.Write(marshaled)
}

type invokeActorRequest struct {
	ServerID  string `json:"server_id"`
	Namespace string `json:"namespace"`
	types.InvokeActorRequest
	// Same data as Payload (in types.InvokeActorRequest), but different field so it doesn't
	// have to be encoded as base64.
	PayloadJSON interface{} `json:"payload_json"`
}

func (s *Server) invoke(w http.ResponseWriter, r *http.Request) {
	if err := ensureHijackable(w); err != nil {
		// Error already written to w.
		return
	}

	jsonBytes, err := ioutil.ReadAll(io.LimitReader(r.Body, 1<<24))
	if err != nil {
		writeStatusCodeForError(w, err)
		w.Write([]byte(err.Error()))
		return
	}

	var req invokeActorRequest
	if err := json.Unmarshal(jsonBytes, &req); err != nil {
		writeStatusCodeForError(w, err)
		w.Write([]byte(err.Error()))
		return
	}

	if len(req.Payload) == 0 && req.PayloadJSON != nil {
		marshaled, err := json.Marshal(req.PayloadJSON)
		if err != nil {
			writeStatusCodeForError(w, err)
			w.Write([]byte(err.Error()))
			return
		}
		req.Payload = marshaled
	}

	result, err := s.environment.InvokeActorStream(
		getContextFromRequest(r), req.Namespace, req.ActorID, req.ModuleID, req.Operation, req.Payload, req.CreateIfNotExist)
	if err != nil {
		writeStatusCodeForError(w, err)
		w.Write([]byte(err.Error()))
		return
	}

	copyResultIntoStreamAndCloseResult(w, result)
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

func (s *Server) invokeDirect(w http.ResponseWriter, r *http.Request) {
	if err := ensureHijackable(w); err != nil {
		// Error already written to w.
		return
	}

	jsonBytes, err := ioutil.ReadAll(io.LimitReader(r.Body, 1<<24))
	if err != nil {
		writeStatusCodeForError(w, err)
		w.Write([]byte(err.Error()))
		return
	}

	var req invokeActorDirectRequest
	if err := json.Unmarshal(jsonBytes, &req); err != nil {
		writeStatusCodeForError(w, err)
		w.Write([]byte(err.Error()))
		return
	}

	ref, err := types.NewVirtualActorReference(req.Namespace, req.ModuleID, req.ActorID, uint64(req.Generation))
	if err != nil {
		writeStatusCodeForError(w, err)
		w.Write([]byte(err.Error()))
		return
	}

	result, err := s.environment.InvokeActorDirectStream(
		getContextFromRequest(r), req.VersionStamp, req.ServerID, req.ServerVersion, ref,
		req.Operation, req.Payload, req.CreateIfNotExist)
	if err != nil {
		writeStatusCodeForError(w, err)
		w.Write([]byte(err.Error()))
		return
	}

	copyResultIntoStreamAndCloseResult(w, result)
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

func (s *Server) invokeWorker(w http.ResponseWriter, r *http.Request) {
	if err := ensureHijackable(w); err != nil {
		// Error already written to w.
		return
	}

	jsonBytes, err := ioutil.ReadAll(io.LimitReader(r.Body, 1<<24))
	if err != nil {
		writeStatusCodeForError(w, err)
		w.Write([]byte(err.Error()))
		return
	}

	var req invokeWorkerRequest
	if err := json.Unmarshal(jsonBytes, &req); err != nil {
		writeStatusCodeForError(w, err)
		w.Write([]byte(err.Error()))
		return
	}

	result, err := s.environment.InvokeWorkerStream(
		getContextFromRequest(r), req.Namespace, req.ModuleID, req.Operation, req.Payload, req.CreateIfNotExist)
	if err != nil {
		writeStatusCodeForError(w, err)
		w.Write([]byte(err.Error()))
		return
	}

	copyResultIntoStreamAndCloseResult(w, result)
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

func writeStatusCodeForError(w http.ResponseWriter, err error) {
	var httpErr HTTPError
	if errors.As(err, &httpErr) {
		w.WriteHeader(httpErr.HTTPStatusCode())
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func copyResultIntoStreamAndCloseResult(
	w http.ResponseWriter,
	result io.ReadCloser,
) {
	defer result.Close()

	w.WriteHeader(200)

	if _, err := io.Copy(w, result); err != nil {
		// Same comment as above.
		terminateConnection(w)
	}
}

func getContextFromRequest(r *http.Request) context.Context {
	timeout := DefaultHTTPRequestTimeout

	if headerValue := r.Header.Get(types.HTTPHeaderTimeout); headerValue != "" {
		headerTimeout, err := time.ParseDuration(headerValue)
		if err == nil {
			timeout = headerTimeout
		}
	}

	ctx, _ := context.WithTimeout(r.Context(), timeout)
	return ctx
}
