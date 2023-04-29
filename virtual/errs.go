package virtual

import (
	"errors"
	"fmt"
	"net/http"
)

var (
	statusCodeToErrorWrapper = map[int]func(err error) error{
		410: NewBlacklistedActivationError,
	}
)

// HTTPError is the interface implemented by errors that map to a specific
// status code. It should be used in conjunction with statusCodeToErrorWrapper
// so that the status code is automatically set on the server, and the status
// code is automatically translated back into the appropriate error wrapped by
// the client.
type HTTPError interface {
	StatusCode() int
}

// BlacklistedActivationErr indicates that the actor activation has been
// blacklisted on this specific server temporarily (usually due to resource
// usage or balancing reasons).
type BlacklistedActivationErr struct {
	err error
}

// NewBlacklistedActivationError creates a new BlacklistedActivationErr.
func NewBlacklistedActivationError(err error) error {
	return BlacklistedActivationErr{err: err}
}

func (b BlacklistedActivationErr) Error() string {
	return fmt.Sprintf("BlacklistedActivationError: %s", b.err.Error())
}

func (b BlacklistedActivationErr) Is(target error) bool {
	if target == nil {
		return false
	}

	_, ok1 := target.(*BlacklistedActivationErr)
	_, ok2 := target.(BlacklistedActivationErr)
	return ok1 || ok2
}

func (b BlacklistedActivationErr) HTTPStatusCode() int {
	return http.StatusGone
}

// IsBlacklistedActivationError returns a boolean indicating whether the error
// was caused by the actor being blacklisted from being activated on the server.
func IsBlacklistedActivationError(err error) bool {
	return errors.Is(err, BlacklistedActivationErr{})
}
