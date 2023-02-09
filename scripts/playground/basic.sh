set -e

curl -w "\n" -X POST "http://localhost:9090/api/v1/register-module" -H "namespace: playground" -H "module_id: test_util" --data-binary "@testdata/tinygo/util/main.wasm" && echo ""

curl -w "\n" -X POST "http://localhost:9090/api/v1/invoke-actor" -H 'Content-Type: application/json' -d '{"namespace":"playground", "operation":"inc", "actor_id":"test_utils_actor_1", "module_id":"test_util"}' && echo ""

curl -w "\n" -X POST "http://localhost:9090/api/v1/invoke-actor" -H 'Content-Type: application/json' -d '{"namespace":"playground", "operation":"inc", "actor_id":"test_utils_actor_1", "module_id":"test_util"}' && echo ""

curl -w "\n" -X POST "http://localhost:9090/api/v1/invoke-actor" -H 'Content-Type: application/json' -d '{"namespace":"playground", "operation":"inc", "actor_id":"test_utils_actor_1", "module_id":"test_util"}' && echo ""
