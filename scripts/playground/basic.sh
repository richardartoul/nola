set -e

curl -X POST "http://localhost:9090/api/v1/register-module" -H "namespace: playground" -H "module_id: test_util" --data-binary "@testdata/tinygo/util/main.wasm" && echo ""
curl -X POST "http://localhost:9090/api/v1/create-actor" -H 'Content-Type: application/json' -d '{"namespace":"playground", "module_id":"test_util", "actor_id":"test_utils_actor_1"}' && echo ""
curl -X POST "http://localhost:9090/api/v1/invoke" -H 'Content-Type: application/json' -d '{"namespace":"playground", "operation":"inc", "actor_id":"test_utils_actor_1"}' && echo ""