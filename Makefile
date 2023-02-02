compile-wasm:
	tinygo build -wasm-abi=generic -target=wasi -o testdata/tinygo/util/main.wasm testdata/tinygo/util/main.go
	tinygo build -wasm-abi=generic -target=wasi -o examples/semaphore/main.wasm examples/semaphore/main.go

test:
	go test ./...

run-server-local-registry:
	go run cmd/app/main.go --discoveryType=localhost --registryBackend=memory

run-server-foundationdb-0:
	go run cmd/app/main.go --discoveryType=localhost --registryBackend=foundationdb --port=9090

run-server-foundationdb-1:
	go run cmd/app/main.go --discoveryType=localhost --registryBackend=foundationdb --port=9091

run-server-foundationdb-2:
	go run cmd/app/main.go --discoveryType=localhost --registryBackend=foundationdb --port=9092

run-wasm-playground:
	bash ./scripts/playground/basic.sh

run-example-semaphore:
	bash ./examples/semaphore/register_module.sh
	bash ./examples/semaphore/create_actors.sh
	bash ./examples/semaphore/acquire.sh
