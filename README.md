# Warehouse delivery

Reads experiment events from the `external_warehouse_events` Kafka topic and inserts each customer's events into their own data warehouse. If a customer's warehouse rejects their events, the connection shows as errored in the dashboard and the events are written to the `external_warehouse_events_retry` topic. Nothing consumes the retry topic yet. Connection targets come from, and delivery status goes back to, the ingestion Redis the API maintains; this service never touches Postgres.

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
| `EXTERNAL_WAREHOUSE_RETRY_TOPIC` | `external_warehouse_events_retry` | Topic failed deliveries are written to |
| `KAFKA_CONSUMER_GROUP` | `warehouse-delivery` | Consumer group |
| `BATCH_MAX_RECORDS` | `5000` | Records per consumed batch |
| `BATCH_MAX_WAIT_MS` | `5000` | Longest wait for a batch to fill |
| `DELIVERY_CONCURRENCY` | `16` | Customers inserted at the same time within a batch |

## Kafka topics

The service does not create topics. Before starting it, create the retry topic (`EXTERNAL_WAREHOUSE_RETRY_TOPIC`) with the same partition count as the events topic, and give the Kafka user write access to it. If the topic is missing, the first failed delivery stops the service without committing the batch.

## Running the image

```
docker build -t warehouse-delivery .
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 -e KAFKA_AUTH=none \
  -e REDIS_URL=redis://host.docker.internal:6379 -e REDIS_CLUSTER=false \
  -e WAREHOUSE_CREDENTIALS_SECRET=dev \
  warehouse-delivery
```

## Deployment

Runs as the `warehouse-delivery` ECS service in the `flagsmith-experimentation` cluster, in staging and production. One Fargate task, no load balancer: it only connects out, to Kafka, the ingestion Redis, and customers' warehouses.

- **Deploys**: a push to `main` deploys staging; a `v*` tag deploys production.
- **Infrastructure**: the ECR repository, log group, security group, execution role, Kafka user and credentials secret all come from `flagsmith/pulumi`. Change them there, not by hand. Only the ECS service itself is created by hand.
- **Secrets**: `WAREHOUSE_CREDENTIALS_SECRET` is shared with the Flagsmith API, which encrypts connection credentials with it. Both must read the same secret, or nothing here can decrypt them.
- **Logs**: `/ecs/warehouse-delivery` in CloudWatch.
- **Rollback**: deploy the previous image. The task keeps no state, and Kafka picks up from the last committed offset.
