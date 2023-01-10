curl -w "\n" -X POST "http://localhost:9090/api/v1/register-module" -H "namespace: playground" -H "module_id: test_util" --data-binary "@testdata/tinygo/util/main.wasm"
