package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/buger/jsonparser"
	"github.com/richardartoul/nola/wapcutils"
	wapc "github.com/wapc/wapc-guest-tinygo"
)

func main() {
	wapc.RegisterFunctions(wapc.Functions{
		// wapcutils.StartupOperationName will automatically be called when the
		// actor is loaded into memory if it is defined, so this is a perfect
		// way to kick off our regularly scheduled housecleaning job.
		wapcutils.StartupOperationName: startup,
		"acquire":                      acquire,
		"release":                      release,
		"houseclean":                   houseclean,
	})
}

type kv struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type kvState struct {
	count int
}

type requestIDState struct {
	kvs       []kv
	expiresAt time.Time
}

var (
	perKeyLimits = map[string]int{
		"user":        5,
		"requestType": 100,
	}

	kvCounts = map[kv]kvState{}

	// TODO: This should be a btree ordered by createdAt to make houseclean()
	//       more efficient, but good enough for now.
	requestIDToKVs = map[string]requestIDState{}
)

func startup(payload []byte) ([]byte, error) {
	if err := scheduleNextHouseclean(); err != nil {
		return nil, err
	}
	return nil, nil
}

type acquireRequest struct {
	Keys      []kv   `json:"keys"`
	RequestID string `json:"request_id"`
	TTLMillis int    `json:"ttl_millis"`
}

func acquire(payload []byte) ([]byte, error) {
	req, err := unmarshalAcquireRequest(payload)
	if err != nil {
		return nil, fmt.Errorf("acquire: error unmarshaling acquire request: %w", err)
	}

	if _, ok := requestIDToKVs[string(req.RequestID)]; ok {
		return nil, fmt.Errorf("acquire: requestID: %s already exists", req.RequestID)
	}

	for _, kv := range req.Keys {
		if _, ok := perKeyLimits[kv.Key]; !ok {
			return nil, fmt.Errorf("acquire: unknown semaphore key: %s", kv.Key)
		}
	}

	for i, kv := range req.Keys {
		existing, ok := kvCounts[kv]
		if ok && existing.count >= perKeyLimits[kv.Key] {
			if err := unwind(req, i); err != nil {
				return nil, fmt.Errorf("acquire: error unwinding: %w", err)
			}
			return nil, fmt.Errorf(
				"acquire: limit of %d exceeded for key: %s",
				perKeyLimits[kv.Key], kv.Key)
		}

		existing.count += 1
		kvCounts[kv] = existing
	}

	requestIDToKVs[req.RequestID] = requestIDState{
		kvs:       req.Keys,
		expiresAt: time.Now().Add(time.Duration(req.TTLMillis) * time.Millisecond),
	}

	return nil, nil
}

type releaseRequest struct {
	RequestID string `json:"request_id"`
}

func release(payload []byte) ([]byte, error) {
	var req releaseRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("acquire: error unmarshaling releaseRequest: %w", err)
	}

	kvs, ok := requestIDToKVs[req.RequestID]
	if !ok {
		// Can happen if the request ID was GC"d by the houseclean method.
		return nil, nil
	}

	for _, kv := range kvs.kvs {
		if err := decrKV(kv); err != nil {
			return nil, err
		}
	}

	delete(requestIDToKVs, req.RequestID)

	return nil, nil
}

func houseclean(payload []byte) ([]byte, error) {
	// Schedule the next housecleaning once this one completes.
	defer func() {
		if err := scheduleNextHouseclean(); err != nil {
			panic(err)
		}
	}()

	now := time.Now()
	for requestID, state := range requestIDToKVs {
		if now.Before(state.expiresAt) {
			continue
		}

		for _, kv := range state.kvs {
			if err := decrKV(kv); err != nil {
				return nil, err
			}
		}

		delete(requestIDToKVs, requestID)
	}

	return nil, nil
}

func unwind(req acquireRequest, i int) error {
	if i == 0 {
		return nil
	}

	for _, kv := range req.Keys[:i-1] {
		if err := decrKV(kv); err != nil {
			return err
		}
	}

	return nil
}

func decrKV(kv kv) error {
	existing, ok := kvCounts[kv]
	if !ok {
		return fmt.Errorf("[invariant violated] kv: %v had count of 0 in unwind", kv)
	}
	if existing.count <= 0 {
		return fmt.Errorf(
			"[invariant violated] kv: %v had count of: %d in unwind",
			kv, existing.count)
	}

	existing.count -= 1

	if existing.count <= 0 {
		delete(kvCounts, kv)
	} else {
		kvCounts[kv] = existing
	}

	return nil
}

var scheduleHousecleanBytes = []byte(`
{
	"operation": "houseclean",
	"after_millis": 10000
}`)

func scheduleNextHouseclean() error {
	_, err := wapc.HostCall(
		"wapc", "nola", wapcutils.ScheduleSelfTimerOperationName, scheduleHousecleanBytes)
	if err != nil {
		return fmt.Errorf("error scheduling next houseclean: %w", err)
	}

	return nil
}

// Manually implement JSON unmarshaling because tinygo doesn't support reflection.
func unmarshalAcquireRequest(payload []byte) (acquireRequest, error) {
	var req acquireRequest
	err := jsonparser.ObjectEach(payload, func(key, value []byte, dataType jsonparser.ValueType, offset int) error {
		switch string(key) {
		case "keys":
			jsonparser.ArrayEach(value, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
				var kv kv
				jsonparser.ObjectEach(value, func(key, value []byte, dataType jsonparser.ValueType, offset int) error {
					switch string(key) {
					case "key":
						kv.Key = string(value)
					case "value":
						kv.Value = string(value)
					}
					return nil
				})
				req.Keys = append(req.Keys, kv)
			})
		case "request_id":
			req.RequestID = string(value)
		case "ttl_millis":
			ttlMillis, err := jsonparser.ParseInt(value)
			if err != nil {
				return err
			}
			req.TTLMillis = int(ttlMillis)
		}
		return nil
	})
	if err != nil {
		return acquireRequest{}, err
	}

	return req, nil
}
