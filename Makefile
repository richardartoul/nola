compile-wasm:
	tinygo build -wasm-abi=generic -target=wasi -o testdata/tinygo/util/main.wasm testdata/tinygo/util/main.go

test:
	go test ./...

run-server-local-registry:
	go run cmd/app/main.go --discoveryType=local --registryBackend=local

run-server-foundationdb-0:
	go run cmd/app/main.go --discoveryType=local --registryBackend=foundationdb --port=9090

run-server-foundationdb-1:
	go run cmd/app/main.go --discoveryType=local --registryBackend=foundationdb --port=9091

run-server-foundationdb-2:
	go run cmd/app/main.go --discoveryType=local --registryBackend=foundationdb --port=9092

run-playground:
	bash ./scripts/playground/basic.sh
