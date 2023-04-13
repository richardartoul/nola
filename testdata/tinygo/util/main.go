package main

import (
	"fmt"
	"strconv"

	"github.com/buger/jsonparser"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"
	wapc "github.com/wapc/wapc-guest-tinygo"
)

func main() {
	wapc.RegisterFunctions(wapc.Functions{
		wapcutils.StartupOperationName:  startup,
		wapcutils.ShutdownOperationName: shutdown,
		"getShutdownValue":              getShutdownValue,
		"getInstantiatePayload":         getInstantiatePayload,
		"getStartupWasCalled":           getStartupWasCalled,
		"inc":                           inc,
		"incFast":                       incFast,
		"dec":                           dec,
		"getCount":                      getCount,
		"echo":                          echo,
		"log":                           log,
		"fail":                          fail,
		"kvPutCount":                    putCount,
		"kvPutCountError":               putCountError,
		"kvGet":                         kvGet,
		"fork":                          fork,
		"invokeActor":                   invokeActor,
		"scheduleSelfTimer":             scheduleSelfTimer,
		"invokeCustomHostFn":            invokeCustomHostFn,
	})
}

var (
	count              int64
	instantiatePayload types.InstantiatePayload
	startupWasCalled   = false
	shutdownWasCalled  = false
)

// getInstantiatePayload returns the payload provided to the Startup invocation.
func getInstantiatePayload(payload []byte) ([]byte, error) {
	return []byte(instantiatePayload.Payload), nil
}

// inc increments the actor's in-memory global counter.
func inc(payload []byte) ([]byte, error) {
	count++
	return []byte(fmt.Sprintf("%d", count)), nil
}

// incFast is the same as inc, but it doesn't return a response
// making it zero-alloc.
func incFast(payload []byte) ([]byte, error) {
	count++
	return nil, nil
}

// dec decrements the actor's in-memory global counter.
func dec(payload []byte) ([]byte, error) {
	count--
	return []byte(fmt.Sprintf("%d", count)), nil
}

// getCount gets the current value of the counter.
func getCount(payload []byte) ([]byte, error) {
	return []byte(fmt.Sprintf("%d", count)), nil
}

// echo "echos" the payload back to the host.
func echo(payload []byte) ([]byte, error) {
	wapc.HostCall("wapc", "testing", "echo", payload)
	return payload, nil
}

// log calls the host logging function.
func log(payload []byte) ([]byte, error) {
	wapc.ConsoleLog(string(payload))
	return []byte(""), nil
}

// fail returns an error immediately.
func fail(payload []byte) ([]byte, error) {
	return []byte(""), fmt.Errorf("planned Failure")
}

// putCount stores the current value of count in the actor's durable KV using
// the string in reqPayload as the key.
func putCount(reqPayload []byte) ([]byte, error) {
	var (
		value   = []byte(fmt.Sprintf("%d", count))
		payload = wapcutils.EncodePutPayload(nil, reqPayload, value)
	)
	_, err := wapc.HostCall("wapc", "nola", wapcutils.KVPutOperationName, payload)
	return nil, err
}

// putCountError is the same as putCount, but it forces the actor to return an
// error which makes testing implicit KV transaction rollback easier.
func putCountError(reqPayload []byte) ([]byte, error) {
	var (
		value   = []byte(fmt.Sprintf("%d", count))
		payload = wapcutils.EncodePutPayload(nil, reqPayload, value)
	)
	_, err := wapc.HostCall("wapc", "nola", wapcutils.KVPutOperationName, payload)
	if err == nil {
		return nil, fmt.Errorf("some fake error")
	}
	return nil, err
}

// kvGet is a "pass through" method that simply calls the host's KV Get function.
func kvGet(payload []byte) ([]byte, error) {
	v, err := wapc.HostCall("wapc", "nola", wapcutils.KVGetOperationName, payload)
	if err != nil {
		return nil, err
	}
	// Skip the first byte since that's just a single bit that indicates whether
	// the key existed or not (to distinguish between empty values and value does
	// not exist). First byte 0 == does not exist, first byte 1 == exists.
	return v[1:], nil
}

// fork "forks" the current actor by creating a new actor based on the same module.
// The new actor's ID will be whatever string payload is.
func fork(payload []byte) ([]byte, error) {
	createActorReq := fmt.Sprintf(`{"actor_id":"%s"}`, string(payload))
	return wapc.HostCall("wapc", "nola", wapcutils.CreateActorOperationName, []byte(createActorReq))
}

// invokeActor is a "passthrough" method which just passes through the provided []byte
// payload to the host invoke actor function. This helps us test that actor's can
// communicate with other actors by invoking their operations/functions.
func invokeActor(payload []byte) ([]byte, error) {
	return wapc.HostCall("wapc", "nola", wapcutils.InvokeActorOperationName, payload)
}

// scheduleSelfTimer is a "passthrough" method which just passes through the provided
// []byte payload to the host's ScheduleSelfTimer function. This helps us test that
// actor's can schedule timers for themselves by calling the host function.
func scheduleSelfTimer(payload []byte) ([]byte, error) {
	return wapc.HostCall("wapc", "nola", wapcutils.ScheduleSelfTimerOperationName, payload)
}

func invokeCustomHostFn(payload []byte) ([]byte, error) {
	return wapc.HostCall("wapc", "nola", string(payload), payload)
}

func startup(payload []byte) ([]byte, error) {
	startupWasCalled = true
	err := jsonparser.ObjectEach(payload, func(key, value []byte, dataType jsonparser.ValueType, offset int) error {
		switch string(key) {
		case "IsWorker":
			isWorker, err := strconv.ParseBool(string(value))
			if err != nil {
				return fmt.Errorf("failed to parse IsWorker bool: %w", err)
			}
			instantiatePayload.IsWorker = isWorker
		case "Payload":
			instantiatePayload.Payload = string(value)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to parse InstantiatePayload: %w", err)
	}

	return nil, nil
}

func getStartupWasCalled(payload []byte) ([]byte, error) {
	if startupWasCalled {
		return []byte("true"), nil
	}
	return []byte("false"), nil
}

func shutdown(payload []byte) ([]byte, error) {
	if instantiatePayload.IsWorker {
		shutdownWasCalled = true
		return nil, nil
	}
	_, err := wapc.HostCall("wapc", "nola", wapcutils.KVPutOperationName, wapcutils.EncodePutPayload(nil, []byte("shutdown"), []byte("true")))
	return nil, err
}

func getShutdownValue(payload []byte) ([]byte, error) {
	if instantiatePayload.IsWorker {
		return []byte(strconv.FormatBool(shutdownWasCalled)), nil
	}
	res, err := wapc.HostCall("wapc", "nola", wapcutils.KVGetOperationName, []byte("shutdown"))
	if err != nil {
		return nil, err
	}

	return res[1:], err
}
