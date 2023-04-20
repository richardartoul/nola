package virtual

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/richardartoul/nola/virtual/registry"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

var ErrUnknownMethod = errors.New("unknown method")

func (s *server) wsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

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
		case "register_module":
			result, err = s.handleWsRegisterModule(ctx, request)
		case "invoke":
			result, err = s.handleWsInvoke(ctx, request)
		case "invoke_direct":
			result, err = s.handleWsInvokeDirect(ctx, request)
		case "invoke_worker":
			result, err = s.handleWsInvokeWorker(ctx, request)
		default:
			err = fmt.Errorf("%w: %s", request.Method)
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

func (s *server) handleWsRegisterModule(ctx context.Context, request JsonRpcRequest) (registry.RegisterModuleResult, error) {
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

	return s.handleRegisterModule(ctx, msg)

}

func (s *server) handleWsInvoke(ctx context.Context, request JsonRpcRequest) ([]byte, error) {
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

	result, err := s.handleInvoke(ctx, msg)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(result)
}

func (s *server) handleWsInvokeDirect(ctx context.Context, request JsonRpcRequest) ([]byte, error) {
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

	result, err := s.handleInvokeDirect(ctx, msg)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(result)
}

func (s *server) handleWsInvokeWorker(ctx context.Context, request JsonRpcRequest) ([]byte, error) {
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

	result, err := s.handleInvokeWorker(ctx, msg)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(result)
}
