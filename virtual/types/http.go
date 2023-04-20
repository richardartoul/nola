package types

type RegisterModuleHttpRequest struct {
	Namespace   string `json:"namespace"`
	ModuleID    string `json:"module_id"`
	ModuleBytes []byte `json:"module_bytes"`
}

type InvokeActorHttpRequest struct {
	ServerID  string `json:"server_id"`
	Namespace string `json:"namespace"`
	InvokeActorRequest
	// Same data as Payload (in types.InvokeActorRequest), but different field so it doesn't
	// have to be encoded as base64.
	PayloadJSON interface{} `json:"payload_json"`
}

type InvokeActorDirectHttpRequest struct {
	VersionStamp     int64            `json:"version_stamp"`
	ServerID         string           `json:"server_id"`
	ServerVersion    int64            `json:"server_version"`
	Namespace        string           `json:"namespace"`
	ModuleID         string           `json:"module_id"`
	ActorID          string           `json:"actor_id"`
	Generation       uint64           `json:"generation"`
	Operation        string           `json:"operation"`
	Payload          []byte           `json:"payload"`
	CreateIfNotExist CreateIfNotExist `json:"create_if_not_exist"`
}

type InvokeWorkerHttpRequest struct {
	Namespace string `json:"namespace"`
	// TODO: Allow ModuleID to be omitted if the caller provides a WASMExecutable field which contains the
	//       actual WASM program that should be executed.
	ModuleID         string           `json:"module_id"`
	Operation        string           `json:"operation"`
	Payload          []byte           `json:"payload"`
	CreateIfNotExist CreateIfNotExist `json:"create_if_not_exist"`
}
