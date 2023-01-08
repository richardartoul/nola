package virtual

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/richardartoul/nola/virtual/types"
)

type httpClient struct {
	c *http.Client
}

func (h *httpClient) InvokeRemote(
	ctx context.Context,
	versionStamp int64,
	reference types.ActorReference,
	operation string,
	payload []byte,
) ([]byte, error) {
	ir := invokeDirectRequest{
		VersionStamp: versionStamp,
		ServerID:     reference.ServerID(),
		Namespace:    reference.Namespace(),
		ModuleID:     reference.ModuleID().ID,
		ActorID:      reference.ActorID().ID,
		Generation:   reference.Generation(),
		Operation:    operation,
		Payload:      payload,
	}
	marshaled, err := json.Marshal(&ir)
	if err != nil {
		return nil, fmt.Errorf("HTTPClient: InvokeDirect: error marshaling invokeDirectRequest: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx, "POST",
		fmt.Sprintf("http://%s/api/v1/invoke-direct", reference.Address()),
		bytes.NewReader(marshaled))
	if err != nil {
		return nil, fmt.Errorf("HTTPClient: InvokeDirect: error constructing request: %w", err)
	}

	resp, err := h.c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTPClient: InvokeDirect: error running request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errMsg string
		body, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			errMsg = string(body)
		}
		return nil, fmt.Errorf("HTTPClient: InvokeDirect: error status code: %d, msg: %s", resp.StatusCode, errMsg)
	}

	invokeResp, err := ioutil.ReadAll(io.LimitReader(resp.Body, 1<<26))
	if err != nil {
		return nil, fmt.Errorf("HTTPClient: InvokeDirect: error reading body: %w", err)
	}

	return invokeResp, nil
}

// NewHTTPClient returns a new HTTPClient that implements the RemoteClient interface.
func NewHTTPClient() RemoteClient {
	transport := &http.Transport{
		// Some of this is copy-pasta from http.DefaultTransport.
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:        0, // No limit.
		MaxIdleConnsPerHost: 6500,
		MaxConnsPerHost:     0, // No limit.
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		// Some cloud providers (like GCP) rate-limit connections to 200MiB/s
		// which means if we allow the SDK to use HTTP2 connections and perform
		// multi-plexing our entire application will get throttled to ~200MiB/s
		// regardless of how many parallel streams we open so make sure we disable
		// HTTP2.
		ForceAttemptHTTP2:     false,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		WriteBufferSize:       1 << 18,
		ReadBufferSize:        1 << 18,
	}
	c := &http.Client{Transport: transport}
	return &httpClient{c: c}
}
