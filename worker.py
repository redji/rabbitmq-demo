"""Consume jobs from the task queue, one at a time."""

import argparse
import json
import time

from broker import TASK_QUEUE, connect, setup


def make_handler(name):
    def handle(channel, method, _properties, body):
        job = json.loads(body)
        print(f"[{name}] received {job['id']}: {job['task']}")
        try:
            if job.get("fail"):
                raise RuntimeError("simulated failure")
            time.sleep(float(job.get("seconds", 1)))
        except Exception as exc:
            print(f"[{name}] failed {job['id']}: {exc} -> dead-letter queue")
            # Reject without requeue. The queue's dead-letter settings move the job to tasks.dlq.
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        print(f"[{name}] done {job['id']}")
        # Ack only after the work succeeds. A crash before this redelivers the job.
        channel.basic_ack(delivery_tag=method.delivery_tag)

    return handle


def main():
    parser = argparse.ArgumentParser(description="Consume jobs from the RabbitMQ task queue")
    parser.add_argument("--name", default="worker", help="Name printed in the logs")
    args = parser.parse_args()

    connection = connect()
    channel = connection.channel()
    setup(channel)
    # Fair dispatch: don't hand this worker a second job until it acks the current one.
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=TASK_QUEUE, on_message_callback=make_handler(args.name))

    print(f"[{args.name}] waiting for jobs. Press Ctrl+C to stop.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(f"\n[{args.name}] stopping")
        channel.stop_consuming()
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
