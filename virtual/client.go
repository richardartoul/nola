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

func (h *httpClient) InvokeActorRemote(
	ctx context.Context,
	versionStamp int64,
	reference types.ActorReference,
	operation string,
	payload []byte,
	create types.CreateIfNotExist,
) (io.ReadCloser, error) {
	ir := invokeActorDirectRequest{
		VersionStamp:     versionStamp,
		ServerID:         reference.ServerID(),
		ServerVersion:    reference.ServerVersion(),
		Namespace:        reference.Namespace(),
		ModuleID:         reference.ModuleID().ID,
		ActorID:          reference.ActorID().ID,
		Generation:       reference.Generation(),
		Operation:        operation,
		Payload:          payload,
		CreateIfNotExist: create,
	}
	marshaled, err := json.Marshal(&ir)
	if err != nil {
		return nil, fmt.Errorf("HTTPClient: InvokeDirect: error marshaling invokeActorDirectRequest: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx, "POST",
		fmt.Sprintf("http://%s/api/v1/invoke-actor-direct", reference.Address()),
		bytes.NewReader(marshaled))
	if err != nil {
		return nil, fmt.Errorf("HTTPClient: InvokeDirect: error constructing request: %w", err)
	}

	resp, err := h.c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTPClient: InvokeDirect: error running request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		var errMsg string
		body, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			errMsg = string(body)
		}
		err = fmt.Errorf("HTTPClient: InvokeDirect: error status code: %d, msg: %s", resp.StatusCode, errMsg)

		// This ensures that errors that implement HTTPError *and* have a mapping
		// in statusCodeToErrorWrapper will be converted back to the proper in memory
		// error type if sent by a server to a client.
		if resp.StatusCode == 410 {
			err = NewBlacklistedActivationError(err, reference.ServerID())
		} else if wrapper, ok := statusCodeToErrorWrapper[resp.StatusCode]; ok {
			err = wrapper(err)
		}
		return nil, err
	}

	return resp.Body, nil
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

// noopClient implements RemoteClient, but always returns an error.
type noopClient struct {
}

// newNoopRemoteClient returns a new noopClient that implements RemoteClient.
func newNOOPRemoteClient() RemoteClient {
	return &noopClient{}
}

func (n *noopClient) InvokeActorRemote(
	ctx context.Context,
	versionStamp int64,
	reference types.ActorReference,
	operation string,
	payload []byte,
	create types.CreateIfNotExist,
) (io.ReadCloser, error) {
	return nil, fmt.Errorf(
		"noopClient: tried to invoke actor(%v) remotely using noop client. Instantiate Environment with a real client instead", reference)
}
