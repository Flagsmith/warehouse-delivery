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

## Deployment

Runs as the `warehouse-delivery` ECS service in the `flagsmith-experimentation` cluster, one Fargate task on ARM64, in both the staging and production accounts. There is no load balancer and nothing listens on a port: the task only makes outbound connections, to the Kafka brokers, to the ingestion Redis, and to each customer's warehouse.

Pushing to `main` deploys staging; tagging `v*` deploys production. Both go through `.github/workflows/.reusable-build-push-ecr.yml`, which builds the image, fills its digest into `infrastructure/aws/<environment>/ecs-task-definition-warehouse-delivery.json` and rolls the service.

`KAFKA_USERNAME` and `KAFKA_PASSWORD` come from `AmazonMSK_warehouse-delivery`, a SCRAM user separate from the one the ingestion API produces with, created by the `flagsmith/pulumi` stack. `WAREHOUSE_CREDENTIALS_SECRET` reads the `DJANGO_SECRET_KEY` field of the Flagsmith API's own `ECS-API` secret, because connection credentials are encrypted with a key derived from it — point the two at different values and nothing can be decrypted.

Deploy the API side (Flagsmith/flagsmith) first. Until it is publishing connections to Redis, this service finds no warehouse for the events it reads, and discards them.

Logs go to the `/ecs/warehouse-delivery` CloudWatch group. To roll back, deploy the previous image digest; there is no state in the task, and Kafka replays from the last committed offset.

## Running the image

```
docker build -t warehouse-delivery .
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 -e KAFKA_AUTH=none \
  -e REDIS_URL=redis://host.docker.internal:6379 -e REDIS_CLUSTER=false \
  -e WAREHOUSE_CREDENTIALS_SECRET=dev \
  warehouse-delivery
```
