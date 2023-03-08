package futures

type Future[T any] interface {
	Go(func() (T, error))
	Resolve(result T)
	Reject(err error)
	ResolveOrReject(result T, err error)
	Wait() (result T, err error)
}
