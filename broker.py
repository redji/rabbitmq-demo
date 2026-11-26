"""Shared RabbitMQ connection and queue topology."""

import os
import time

import pika

TASK_QUEUE = "tasks"
DEAD_LETTER_QUEUE = "tasks.dlq"


def _load_dotenv(path=".env"):
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


_load_dotenv()

HOST = os.getenv("RABBITMQ_HOST", "localhost")
PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
USER = os.getenv("RABBITMQ_USER", "demo")
PASSWORD = os.getenv("RABBITMQ_PASSWORD", "demo")
VHOST = os.getenv("RABBITMQ_VHOST", "/")


def connect(retries=30, delay=1.0):
    """Open a blocking connection, waiting while the broker is still starting."""
    credentials = pika.PlainCredentials(USER, PASSWORD)
    parameters = pika.ConnectionParameters(
        host=HOST,
        port=PORT,
        virtual_host=VHOST,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=30,
    )
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return pika.BlockingConnection(parameters)
        except pika.exceptions.AMQPConnectionError as exc:
            last_error = exc
            print(f"Waiting for RabbitMQ at {HOST}:{PORT} ({attempt}/{retries})...")
            time.sleep(delay)
    raise last_error


def setup(channel):
    """Declare a durable work queue and the dead-letter queue it rejects into.

    Re-declaring these queues with different arguments fails if they already
    exist. Delete them in the management UI if you change this topology.
    """
    channel.queue_declare(queue=DEAD_LETTER_QUEUE, durable=True)
    channel.queue_declare(
        queue=TASK_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": DEAD_LETTER_QUEUE,
        },
    )
