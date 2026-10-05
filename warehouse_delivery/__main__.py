import logging
import signal
import sys
import threading
from collections.abc import Callable, Sequence
from functools import partial

import structlog
from confluent_kafka import Consumer, Producer
from psycopg_pool import ConnectionPool

from warehouse_delivery.config import Config
from warehouse_delivery.connection_status import PostgresConnectionStatusWriter
from warehouse_delivery.connections import PostgresWarehouseConnections
from warehouse_delivery.crypto import fernet_from_secret
from warehouse_delivery.delivery import DeliveryService
from warehouse_delivery.kafka import consumer_settings, producer_settings
from warehouse_delivery.loops import run_delivery_loop
from warehouse_delivery.retries import KafkaRetryWriter

logger = structlog.get_logger("warehouse")

DATABASE_POOL_SIZE = 4
DATABASE_CONNECT_TIMEOUT_SECONDS = 5.0


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


def build_database_pool(config: Config) -> ConnectionPool:
    return ConnectionPool(
        config.database_url,
        min_size=1,
        max_size=DATABASE_POOL_SIZE,
        timeout=DATABASE_CONNECT_TIMEOUT_SECONDS,
        open=True,
    )


def run_loops(loops: Sequence[Callable[[], None]], stop: threading.Event) -> None:
    """If one loop raises, the others are told to stop, and its error is raised
    once they have returned."""
    errors: list[BaseException] = []

    def run(loop: Callable[[], None]) -> None:
        try:
            loop()
        except BaseException as exc:
            errors.append(exc)
            stop.set()

    threads = [threading.Thread(target=run, args=(loop,)) for loop in loops]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise errors[0]


def build_delivery_service(
    config: Config,
    connections: PostgresWarehouseConnections,
    status_writer: PostgresConnectionStatusWriter,
) -> DeliveryService:
    return DeliveryService(
        connections=connections,
        status_writer=status_writer,
        retry_writer=KafkaRetryWriter(
            Producer(producer_settings(config)),
            config.retry_topic,
            max_retries=config.max_retries,
        ),
        concurrency=config.delivery_concurrency,
    )


def run_consumer(
    config: Config,
    topic: str,
    group_id: str,
    delivery_service: DeliveryService,
    stop: threading.Event,
) -> None:
    structlog.contextvars.bind_contextvars(consumer__topic=topic)
    consumer = Consumer(consumer_settings(config, group_id=group_id))
    consumer.subscribe([topic])
    try:
        run_delivery_loop(
            consumer,
            delivery_service,
            batch_max_records=config.batch_max_records,
            batch_max_wait_seconds=config.batch_max_wait_seconds,
            retry_delay_seconds=config.retry_delay_seconds,
            stop=stop,
        )
    except BaseException as exc:
        logger.error("service.failed", exc_info=exc)
        raise
    finally:
        consumer.close()


def main() -> None:
    """If either loop fails for a reason on our side, such as Postgres being
    down or a bug, the process exits so ECS restarts it, and Kafka hands back
    the messages that were never marked done."""
    configure_logging()
    config = Config.from_env()
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())

    database_pool = build_database_pool(config)
    connections = PostgresWarehouseConnections(
        database_pool, fernet_from_secret(config.warehouse_credentials_secret)
    )
    status_writer = PostgresConnectionStatusWriter(database_pool)
    # Each loop gets its own producer, so one loop's flush never waits on or
    # reports the other loop's retry messages.
    events_delivery = build_delivery_service(config, connections, status_writer)
    retry_delivery = build_delivery_service(config, connections, status_writer)

    logger.info(
        "service.started",
        events__topic=config.events_topic,
        retry__topic=config.retry_topic,
        consumer__group=config.consumer_group,
        retry__consumer_group=config.retry_consumer_group,
    )
    try:
        run_loops(
            [
                partial(
                    run_consumer,
                    config,
                    config.events_topic,
                    config.consumer_group,
                    events_delivery,
                    stop,
                ),
                partial(
                    run_consumer,
                    config,
                    config.retry_topic,
                    config.retry_consumer_group,
                    retry_delivery,
                    stop,
                ),
            ],
            stop,
        )
    finally:
        database_pool.close()
        logger.info("service.stopped")


if __name__ == "__main__":
    main()
