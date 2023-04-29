package virtual

import "errors"

type BlacklistedActivationErr struct {
	err error
}

// NewBlacklistedActivationError creates a new BlacklistedActivationErr.
func NewBlacklistedActivationError(err error) BlacklistedActivationErr {
	return BlacklistedActivationErr{err: err}
}

func (b BlacklistedActivationErr) Error() string {
	return b.err.Error()
}

func (b BlacklistedActivationErr) Is(target error) bool {
	if target == nil {
		return false
	}

	_, ok1 := target.(*BlacklistedActivationErr)
	_, ok2 := target.(BlacklistedActivationErr)
	return ok1 || ok2
}

// IsBlacklistedActivationError returns a boolean indicating whether the error
// was caused by the actor being blacklisted from being activated on the server.
func IsBlacklistedActivationError(err error) bool {
	return errors.Is(err, BlacklistedActivationErr{})
}
