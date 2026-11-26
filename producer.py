"""Publish jobs onto the durable task queue."""

import argparse
import json
import uuid
from datetime import datetime, timezone

import pika

from broker import TASK_QUEUE, connect, setup

DEMO_JOBS = [
    {"task": "pack-order", "seconds": 1},
    {"task": "charge-payment", "seconds": 2},
    {"task": "send-email", "seconds": 1},
    {"task": "generate-invoice", "seconds": 3},
    {"task": "refund-payment", "seconds": 1, "fail": True},
]


def publish(channel, task, seconds, should_fail=False):
    job = {
        "id": str(uuid.uuid4()),
        "task": task,
        "seconds": seconds,
        "fail": should_fail,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    # The default exchange routes by queue name. Persistent delivery keeps the
    # job on disk with the durable queue until a worker acknowledges it.
    channel.basic_publish(
        exchange="",
        routing_key=TASK_QUEUE,
        body=json.dumps(job),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
        ),
    )
    label = " (will fail)" if should_fail else ""
    print(f"Sent {job['id']}: {task}{label}")
    return job


def main():
    parser = argparse.ArgumentParser(description="Publish jobs to the RabbitMQ task queue")
    parser.add_argument("--task", default="process-order", help="Task name")
    parser.add_argument("--seconds", type=float, default=2, help="Simulated work duration")
    parser.add_argument("--count", type=int, default=1, help="How many messages to send")
    parser.add_argument("--fail", action="store_true", help="Ask the worker to reject the job")
    parser.add_argument("--demo", action="store_true", help="Send a small mixed batch of jobs")
    args = parser.parse_args()

    connection = connect()
    channel = connection.channel()
    setup(channel)

    if args.demo:
        for job in DEMO_JOBS:
            publish(channel, job["task"], job["seconds"], job.get("fail", False))
    else:
        for index in range(args.count):
            name = args.task if args.count == 1 else f"{args.task}-{index + 1}"
            publish(channel, name, args.seconds, args.fail)

    connection.close()


if __name__ == "__main__":
    main()
