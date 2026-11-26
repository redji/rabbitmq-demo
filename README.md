# RabbitMQ work-queue demo

A small producer and worker that share one durable task queue. Jobs stay on disk until a worker acknowledges them. A rejected job is moved to a dead-letter queue instead of being retried forever.

## What you get

- `tasks` — durable work queue
- `tasks.dlq` — dead-letter queue for rejected jobs
- Persistent messages and manual acknowledgements
- Prefetch of 1, so several workers share the load evenly

## Run it

RabbitMQ (management UI on http://localhost:15672, user `demo`, password `demo`):

```powershell
docker compose up -d
```

Python app:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Send a mixed batch, then start a worker (use a second terminal for the worker, with the venv activated):

```powershell
python producer.py --demo
python worker.py --name worker-1
```

Open another terminal and start `python worker.py --name worker-2` before the first worker finishes. The jobs are split between them.

Send your own job:

```powershell
python producer.py --task "resize-photo" --seconds 4
python producer.py --task "bad-job" --fail
```

The failed job shows up in `tasks.dlq` in the management UI (Queues and Streams).

## Connection settings

Defaults match `docker-compose.yml` (`demo` / `demo` on `localhost:5672`). Copy `.env.example` to `.env` to override them. The built-in `guest` user cannot connect from the host into the Docker broker, so this demo uses its own user.

## Stop

```powershell
docker compose down
```

Add `-v` if you also want to delete the broker data volume.
