# Gloci

`gloci` is a local GCP emulator written in Nim.
Current MVP supports in-memory emulation for Cloud Storage, Pub/Sub, and Cloud Scheduler.

## Quick Start

```bash
docker compose up --build
```

Server starts at `http://localhost:8080`.

Health check:

```bash
curl -s http://localhost:8080/healthz
```

## API (MVP)

### Storage

Create bucket:

```bash
curl -s -X PUT http://localhost:8080/storage/v1/buckets/my-bucket
```

Put object:

```bash
curl -s -X PUT --data 'hello world' \
  http://localhost:8080/storage/v1/buckets/my-bucket/objects/greeting.txt
```

Get object:

```bash
curl -s http://localhost:8080/storage/v1/buckets/my-bucket/objects/greeting.txt
```

List objects:

```bash
curl -s http://localhost:8080/storage/v1/buckets/my-bucket/objects
```

### Pub/Sub

Create topic:

```bash
curl -s -X PUT http://localhost:8080/pubsub/v1/topics/my-topic
```

Create subscription:

```bash
curl -s -X PUT \
  -H 'Content-Type: application/json' \
  -d '{"topic":"my-topic"}' \
  http://localhost:8080/pubsub/v1/subscriptions/my-sub
```

Publish:

```bash
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"data":"hello"}]}' \
  http://localhost:8080/pubsub/v1/topics/my-topic/publish
```

Pull:

```bash
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"maxMessages":10}' \
  http://localhost:8080/pubsub/v1/subscriptions/my-sub/pull
```

### Scheduler

Create job (every 5s):

```bash
curl -s -X PUT \
  -H 'Content-Type: application/json' \
  -d '{"topic":"my-topic","payload":"from scheduler","everySeconds":5}' \
  http://localhost:8080/scheduler/v1/jobs/job-1
```

Run manually:

```bash
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{}' \
  http://localhost:8080/scheduler/v1/jobs/job-1/run
```

List jobs:

```bash
curl -s http://localhost:8080/scheduler/v1/jobs
```

## Notes
- Data is in-memory only; restarting the process clears all resources.
- API shapes are intentionally simple for MVP and are not yet fully GCP-compatible.
