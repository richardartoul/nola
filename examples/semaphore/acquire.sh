curl -w "\n" -X POST "http://localhost:9090/api/v1/invoke-actor" -H "Content-Type: application/json" -d "{\"namespace\":\"examples\", \"operation\":\"acquire\", \"actor_id\":\"tenant_1\", \"module_id\":\"semaphore\", \"payload_json\": {\"keys\": [{\"key\": \"user\", \"value\": \"user1\"}], \"request_id\": \"$RANDOM\", \"TTLMillis\": 30000}}"

c\url -w "\n" -X POST "http://localhost:9090/api/v1/invoke-actor" -H "Content-Type: application/json" -d "{\"namespace\":\"examples\", \"operation\":\"acquire\", \"actor_id\":\"tenant_1\", \"module_id\":\"semaphore\", \"payload_json\": {\"keys\": [{\"key\": \"user\", \"value\": \"user1\"}], \"request_id\": \"$RANDOM\", \"TTLMillis\": 30000}}"

curl -w "\n" -X POST "http://localhost:9090/api/v1/invoke-actor" -H "Content-Type: application/json" -d "{\"namespace\":\"examples\", \"operation\":\"acquire\", \"actor_id\":\"tenant_1\", \"module_id\":\"semaphore\", \"payload_json\": {\"keys\": [{\"key\": \"user\", \"value\": \"user1\"}], \"request_id\": \"$RANDOM\", \"TTLMillis\": 30000}}"

curl -w "\n" -X POST "http://localhost:9090/api/v1/invoke-actor" -H "Content-Type: application/json" -d "{\"namespace\":\"examples\", \"operation\":\"acquire\", \"actor_id\":\"tenant_1\", \"module_id\":\"semaphore\", \"payload_json\": {\"keys\": [{\"key\": \"user\", \"value\": \"user1\"}], \"request_id\": \"$RANDOM\", \"TTLMillis\": 30000}}"

curl -w "\n" -X POST "http://localhost:9090/api/v1/invoke-actor" -H "Content-Type: application/json" -d "{\"namespace\":\"examples\", \"operation\":\"acquire\", \"actor_id\":\"tenant_1\", \"module_id\":\"semaphore\", \"payload_json\": {\"keys\": [{\"key\": \"user\", \"value\": \"user1\"}], \"request_id\": \"$RANDOM\", \"TTLMillis\": 30000}}"
