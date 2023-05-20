package types

// ReferenceType is an enum type that indicates what the underlying type of Reference is,
// see the different ReferenceType's below.
type ReferenceType string

const (
	// ReferenceTypeLocal indicates the actor is "local" to the current process. Mainly
	// used for benchmarking and tests.
	ReferenceTypeLocal ReferenceType = "local"
	// ReferenceTypeRemoteHTTP indicates the actor is remote and must be accessed via
	// an HTTP call to the associated address.
	ReferenceTypeRemoteHTTP ReferenceType = "remote-http"
)

func StringSliceToSet(slice []string) map[string]bool {
	if slice == nil {
		return make(map[string]bool)
	}

	set := make(map[string]bool, len(slice))
	for _, item := range slice {
		set[item] = true
	}
	return set
}
