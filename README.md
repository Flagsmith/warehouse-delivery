# Warehouse delivery

Reads experiment events from the `external_warehouse_events` Kafka topic and inserts each customer's events into their own data warehouse. If a customer's warehouse rejects their events, the connection shows as errored in the dashboard and the events are written to the `external_warehouse_events_retry` topic. A second loop in the same process reads that topic and tries each event again once `RETRY_DELAY_MS` has passed since it failed, up to `MAX_RETRIES` times, after which it is dropped and logged. Connection targets come from the `experimentation_delivery_connections` db view in Flagsmith's Postgres, and each connection's delivery status is written to the `experimentation_warehousedeliverystatus` table there.

## Local development

Requires [uv](https://github.com/astral-sh/uv) and Python 3.14.

```
make install            # first time: make lock, then make install
make lint
make typecheck
make test
make run
```

`make test` needs a Postgres at `DATABASE_URL`; each database test works in its own throwaway schema.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | required | Broker list, as for the ingestion server |
| `KAFKA_AUTH` | `scram` | `scram` for MSK with SASL/SCRAM over TLS, `none` for local brokers |
| `KAFKA_USERNAME`, `KAFKA_PASSWORD` | required with `scram` | SCRAM credentials |
| `DATABASE_URL` | required | Flagsmith's Postgres, for connections and delivery status |
| `WAREHOUSE_CREDENTIALS_SECRET` | required | Same value as the API; derives the key that decrypts connection credentials |
| `EXTERNAL_WAREHOUSE_TOPIC` | `external_warehouse_events` | Topic to deliver from |
| `EXTERNAL_WAREHOUSE_RETRY_TOPIC` | `external_warehouse_events_retry` | Topic failed deliveries are written to |
| `KAFKA_CONSUMER_GROUP` | `warehouse-delivery` | Consumer group |
| `KAFKA_RETRY_CONSUMER_GROUP` | `warehouse-delivery-retry` | Consumer group for the retry topic |
| `RETRY_DELAY_MS` | `300000` | How long after a failure an event is tried again; at most 300000 |
| `MAX_RETRIES` | `12` | Retries per event before it is dropped |
| `BATCH_MAX_RECORDS` | `5000` | Records per consumed batch |
| `BATCH_MAX_WAIT_MS` | `5000` | Longest wait for a batch to fill |
| `DELIVERY_CONCURRENCY` | `16` | Customers inserted at the same time within a batch |

## Kafka topics

The service does not create topics. Before starting it, create the retry topic (`EXTERNAL_WAREHOUSE_RETRY_TOPIC`) with the same partition count as the events topic, and give the Kafka user read and write access to it and access to the `KAFKA_RETRY_CONSUMER_GROUP` consumer group. If the topic is missing, the first failed delivery stops the service without committing the batch.

## Running the image

```
docker build -t warehouse-delivery .
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 -e KAFKA_AUTH=none \
  -e DATABASE_URL=postgresql://postgres:password@host.docker.internal:5432/flagsmith \
  -e WAREHOUSE_CREDENTIALS_SECRET=dev \
  warehouse-delivery
```

## Kubernetes

The service serves no traffic, so it needs no readiness probe. It exits on its own failures, and Kubernetes restarts it. A liveness probe catches the remaining case: a loop that hangs without exiting. Each loop touches `/tmp/heartbeat-events` or `/tmp/heartbeat-retry` on every pass, including empty ones. A pass is built to finish within Kafka's ten-minute poll interval, so a file older than that means its loop is stuck.

Don't probe Kafka or Postgres: an outage would restart every pod without fixing anything.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: warehouse-delivery
spec:
  replicas: 1
  selector:
    matchLabels:
      app: warehouse-delivery
  template:
    metadata:
      labels:
        app: warehouse-delivery
    spec:
      terminationGracePeriodSeconds: 60  # lets the current batch finish
      containers:
        - name: warehouse-delivery
          image: flagsmith/warehouse-delivery:<version>
          envFrom:
            - secretRef:
                name: warehouse-delivery
          livenessProbe:
            exec:
              command:
                - sh
                - -c
                - >-
                  for f in /tmp/heartbeat-events /tmp/heartbeat-retry; do
                  [ $(( $(date +%s) - $(stat -c %Y "$f") )) -lt 600 ] || exit 1;
                  done
            initialDelaySeconds: 30
            periodSeconds: 30
            failureThreshold: 2
          securityContext:
            readOnlyRootFilesystem: true
          volumeMounts:
            - name: tmp
              mountPath: /tmp
      volumes:
        - name: tmp
          emptyDir: {}
```

The `warehouse-delivery` secret holds the variables under [Configuration](#configuration). The `emptyDir` is only needed with `readOnlyRootFilesystem`.

## Deployment

Runs as the `warehouse-delivery` ECS service in the `flagsmith-experimentation` cluster, in staging and production. One Fargate task, no load balancer: it only connects out, to Kafka, Flagsmith's Postgres, and customers' warehouses.

- **Deploys**: a push to `main` deploys staging; a `v*` tag deploys production.
- **Infrastructure**: the ECR repository, log group, security group, execution role, Kafka user and credentials secret all come from `flagsmith/pulumi`. Change them there, not by hand. Only the ECS service itself is created by hand.
- **Secrets**: `WAREHOUSE_CREDENTIALS_SECRET` is shared with the Flagsmith API, which encrypts connection credentials with it. Both must read the same secret, or nothing here can decrypt them.
- **Logs**: `/ecs/warehouse-delivery` in CloudWatch.
- **Rollback**: deploy the previous image. The task keeps no state, and Kafka picks up from the last committed offset.
