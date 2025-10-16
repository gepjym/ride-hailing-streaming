# Ride-hailing Streaming (Postgres → Debezium → Kafka → Flink → Postgres/ES)

## Run
```bash
docker compose up -d
bash scripts/register_connector.sh

# Build Flink job
cd flink-job && mvn -q -DskipTests package && cd ..

# Submit job
docker exec -it flink-jobmanager flink run -c com.ridehailing.RideHailingDataProcessor /jars/flink-job-1.0.0.jar

# Generate data
python3 generator/data_generator.py
```
