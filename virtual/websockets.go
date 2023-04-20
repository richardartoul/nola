package virtual

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type WebsocketServerHandler struct {
	s *server
}

func (s *WebsocketServerHandler) handler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// TODO: log errors

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}

	var result any

	for {
		var request JsonRpcRequest
		err = wsjson.Read(ctx, c, &request)
		if err != nil {
			return
		}

		switch request.Method {
		case "register-module":
			result, err = s.handleRegisterModule(ctx, request)
		}

		response := JsonRpcResponse{VersionTag: request.VersionTag, ID: request.ID}
		if err != nil {
			response.Error.Code = websocket.StatusInternalError
			response.Error.Message = err.Error()
		} else {
			response.Result = result
		}

		if err := wsjson.Write(ctx, c, response); err != nil {
			return
		}
	}

}

func (s *WebsocketServerHandler) handleRegisterModule(ctx context.Context, request JsonRpcRequest) (registry.RegisterModuleResult, error) {
	var (
		params []registerModuleMessage
		msg    registerModuleMessage
	)

	if err := json.Unmarshal(request.Params, &params); err != nil {
		return registry.RegisterModuleResult{}, err
	}

	if n := len(params); n != 1 {
		return registry.RegisterModuleResult{}, fmt.Errorf("invalid number of params: expected 1 - received: %d", n)
	}

	return s.s.registry.RegisterModule(ctx, msg.Namespace, msg.ModuleID, msg.ModuleBytes, registry.ModuleOptions{})

}

func (s *WebsocketServerHandler) handleInvokeActorStream(ctx context.Context, request JsonRpcRequest) ([]byte, error) {
	var (
		params []invokeActorRequest
		msg    invokeActorRequest
	)

	if err := json.Unmarshal(request.Params, &params); err != nil {
		return nil, err
	}

	if n := len(params); n != 1 {
		return nil, fmt.Errorf("invalid number of params: expected 1 - received: %d", n)
	}

	result, err := s.s.environment.InvokeActorStream(ctx, msg.Namespace, msg.ActorID, msg.ModuleID, msg.Operation, msg.Payload, msg.CreateIfNotExist)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(result)
}

func (s *WebsocketServerHandler) handleInvokeDirect(ctx context.Context, request JsonRpcRequest) ([]byte, error) {
	var (
		params []invokeActorDirectRequest
		msg    invokeActorDirectRequest
	)

	if err := json.Unmarshal(request.Params, &params); err != nil {
		return nil, err
	}

	if n := len(params); n != 1 {
		return nil, fmt.Errorf("invalid number of params: expected 1 - received: %d", n)
	}

	ref, err := types.NewVirtualActorReference(msg.Namespace, msg.ModuleID, msg.ActorID, msg.Generation)
	if err != nil {
		return nil, fmt.Errorf("failed to crate new actor: %w", err)
	}

	result, err := s.s.environment.InvokeActorDirectStream(ctx, msg.VersionStamp, msg.ServerID, msg.ServerVersion, ref, msg.Operation, msg.Payload, msg.CreateIfNotExist)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(result)
}

func (s *WebsocketServerHandler) handleInvokeWorker(ctx context.Context, request JsonRpcRequest) ([]byte, error) {
	var (
		params []invokeWorkerRequest
		msg    invokeWorkerRequest
	)

	if err := json.Unmarshal(request.Params, &params); err != nil {
		return nil, err
	}

	if n := len(params); n != 1 {
		return nil, fmt.Errorf("invalid number of params: expected 1 - received: %d", n)
	}

	// TODO: This should be configurable, probably in a header with some maximum.
	ctx, cc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cc()

	result, err := s.s.environment.InvokeWorkerStream(ctx, msg.Namespace, msg.ModuleID, msg.Operation, msg.Payload, msg.CreateIfNotExist)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(result)
}

type registerModuleMessage struct {
	Namespace, ModuleID string
	ModuleBytes         []byte
}
