import logging
import signal
import sys
import threading
from typing import Any

import structlog
from confluent_kafka import Consumer, Producer
from redis import Redis
from redis.cluster import RedisCluster

from warehouse_delivery.config import Config
from warehouse_delivery.connection_status import RedisConnectionStatusWriter
from warehouse_delivery.connections import RedisWarehouseConnections
from warehouse_delivery.crypto import fernet_from_secret
from warehouse_delivery.delivery import DeliveryService
from warehouse_delivery.kafka import consumer_settings, producer_settings
from warehouse_delivery.loops import run_delivery_loop
from warehouse_delivery.retries import KafkaRetryWriter

logger = structlog.get_logger("warehouse")

REDIS_SOCKET_TIMEOUT_SECONDS = 2.0


def configure_logging() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(sys.stdout),
        cache_logger_on_first_use=True,
    )


def build_redis_client(config: Config) -> Any:
    client_class = RedisCluster if config.redis_cluster else Redis
    return client_class.from_url(
        config.redis_url,
        socket_timeout=REDIS_SOCKET_TIMEOUT_SECONDS,
        socket_keepalive=True,
    )


def main() -> None:
    """Runs the delivery loop until the process is told to stop with SIGINT or
    SIGTERM. If the loop fails for a reason on our side, such as Redis being
    down or a bug, the error is logged and the process exits so ECS restarts
    it; Kafka then hands back the messages that were never marked done."""
    configure_logging()
    config = Config.from_env()
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())

    redis_client = build_redis_client(config)
    delivery_service = DeliveryService(
        connections=RedisWarehouseConnections(
            redis_client, fernet_from_secret(config.warehouse_credentials_secret)
        ),
        status_writer=RedisConnectionStatusWriter(redis_client),
        retry_writer=KafkaRetryWriter(
            Producer(producer_settings(config)), config.retry_topic
        ),
        concurrency=config.delivery_concurrency,
    )
    consumer = Consumer(consumer_settings(config))
    consumer.subscribe([config.events_topic])

    logger.info(
        "service.started",
        events__topic=config.events_topic,
        retry__topic=config.retry_topic,
        consumer__group=config.consumer_group,
    )
    try:
        run_delivery_loop(
            consumer,
            delivery_service,
            batch_max_records=config.batch_max_records,
            batch_max_wait_seconds=config.batch_max_wait_seconds,
            stop=stop,
        )
    except BaseException as exc:
        logger.error("service.failed", exc_info=exc)
        raise
    finally:
        consumer.close()
        logger.info("service.stopped")


if __name__ == "__main__":
    main()
