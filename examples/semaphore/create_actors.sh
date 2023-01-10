curl -w "\n" -X POST "http://localhost:9090/api/v1/create-actor" -H 'Content-Type: application/json' -d '{"namespace":"examples", "module_id":"semaphore", "actor_id":"tenant_1"}'

curl -w "\n" -X POST "http://localhost:9090/api/v1/create-actor" -H 'Content-Type: application/json' -d '{"namespace":"examples", "module_id":"semaphore", "actor_id":"tenant_2"}'

curl -w "\n" -X POST "http://localhost:9090/api/v1/create-actor" -H 'Content-Type: application/json' -d '{"namespace":"examples", "module_id":"semaphore", "actor_id":"tenant_3"}'
