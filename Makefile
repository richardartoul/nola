compile-wasm:
	tinygo build -wasm-abi=generic -target=wasi -o testdata/tinygo/util/main.wasm testdata/tinygo/util/main.go

test:
	go test ./...

server:
	go run cmd/app/main.go

run-playground:
	bash ./scripts/playground/basic.sh
