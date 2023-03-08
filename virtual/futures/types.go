package futures

// Future is the interface for a Future. It behaves similarly to a Promise
// or Future in other programming languages.
type Future[T any] interface {
	// GoSync executes the provided function in the current Goroutine and
	// resolves the future with the return result.
	GoSync(func() (T, error))
	// GoAsync is the same as GoSync, but it runs the function in a new
	// goroutine and doesn't block the caller.
	GoAsync(func() (T, error))
	// Resolve resolves the future immediately with result.
	Resolve(result T)
	// Reject rejects the future immediately with an error.
	Reject(err error)
	// ResolveOrReject combines Resolve and Reject into one method for
	// convenience.
	ResolveOrReject(result T, err error)
	// Wait waits for the future to resolve and returns the tuple of
	// result/error.
	Wait() (result T, err error)
}
