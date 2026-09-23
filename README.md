# Warehouse delivery

Reads experiment events from the `external_warehouse_events` Kafka topic and inserts each customer's events into their own data warehouse. If a customer's warehouse rejects their events, the connection shows as errored in the dashboard and those events are lost; there is no retry yet. Connection targets come from, and delivery status goes back to, the ingestion Redis the API maintains; this service never touches Postgres.

## Local development

Requires [uv](https://github.com/astral-sh/uv) and Python 3.14.

```
make install            # first time: make lock, then make install
make lint
make typecheck
make test
make run
```

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | required | Broker list, as for the ingestion server |
| `KAFKA_AUTH` | `scram` | `scram` for MSK with SASL/SCRAM over TLS, `none` for local brokers |
| `KAFKA_USERNAME`, `KAFKA_PASSWORD` | required with `scram` | SCRAM credentials |
| `REDIS_URL` | required | Ingestion Redis, as for the ingestion server |
| `REDIS_CLUSTER` | `true` | `false` for a single-node Redis in local runs |
| `WAREHOUSE_CREDENTIALS_SECRET` | required | Same value as the API; derives the key that decrypts connection credentials |
| `EXTERNAL_WAREHOUSE_TOPIC` | `external_warehouse_events` | Topic to deliver from |
| `KAFKA_CONSUMER_GROUP` | `warehouse-delivery` | Consumer group |
| `BATCH_MAX_RECORDS` | `5000` | Records per consumed batch |
| `BATCH_MAX_WAIT_MS` | `5000` | Longest wait for a batch to fill |
| `DELIVERY_CONCURRENCY` | `16` | Customers inserted at the same time within a batch |

## Running the image

```
docker build -t warehouse-delivery .
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 -e KAFKA_AUTH=none \
  -e REDIS_URL=redis://host.docker.internal:6379 -e REDIS_CLUSTER=false \
  -e WAREHOUSE_CREDENTIALS_SECRET=dev \
  warehouse-delivery
```
