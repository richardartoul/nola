package virtual

import (
	"context"
	"net/http"

	"github.com/richardartoul/nola/virtual/types"
)

type httpClient struct {
	address string
	c       *http.Client
}

func (h *httpClient) InvokeDirect(
	ctx context.Context,
	versionStamp int64,
	serverID string,
	reference types.ActorReferenceVirtual,
	operation string,
	payload []byte,
) ([]byte, error) {
	// ir := invokeRequest{
	// 	ServerID:  serverID,
	// 	Namespace: reference.Namespace(),
	// 	ActorID:   reference.ActorID().ID,
	// 	Operation: operation,
	// 	Payload:   payload,
	// }
	// req, err := http.NewRequestWithContext(
	// 	ctx, "POST",
	// 	fmt.Sprintf("http://%s/api/v1/put?block_id=%d", h.address, blockID),
	// 	bytes.NewReader(data))
	// if err != nil {
	// 	return err
	// }

	// resp, err := h.client.Do(req)
	// if err != nil {
	// 	return err
	// }
	// resp.Body.Close()

	// if resp.StatusCode != http.StatusOK {
	// 	var errMsg string
	// 	body, err := ioutil.ReadAll(resp.Body)
	// 	if err == nil {
	// 		errMsg = string(body)
	// 	}
	// 	return fmt.Errorf("HttpClient: Put: error status code: %d, msg: %s", resp.StatusCode, errMsg)
	// }

	return nil, nil
}

func newHTTPClient(address string) RemoteClient {
	// blob.ConfigureDefaultHTTPClientLargeStreams()
	transport := http.DefaultTransport.(*http.Transport)
	c := &http.Client{Transport: transport}
	return &httpClient{address: address, c: c}
}
