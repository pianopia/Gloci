# Gloci

`gloci` is a local GCP emulator written in Nim.
Current MVP supports Cloud Storage, Pub/Sub, and Cloud Scheduler, with optional local file persistence for Storage and queued Pub/Sub messages.

## Quick Start

```bash
docker compose up --build
```

Server starts at `http://localhost:8080`.

Health check:

```bash
curl -s http://localhost:8080/healthz
```

## Persistence (Optional)

Set `GLOCI_DATA_FILE` to enable persistence to a JSON file.

Local run example:

```bash
GLOCI_DATA_FILE=./data/state.json ./gloci
```

Docker Compose example:

```bash
docker compose up --build
```

The provided compose file stores persisted state at `/data/state.json` in the `gloci_data` volume.

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

Acknowledge:

```bash
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"ackIds":["my-sub:1"]}' \
  http://localhost:8080/pubsub/v1/subscriptions/my-sub/acknowledge
```

Modify ack deadline (seconds, `0` means requeue immediately):

```bash
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"ackIds":["my-sub:1"],"ackDeadlineSeconds":30}' \
  http://localhost:8080/pubsub/v1/subscriptions/my-sub/modifyAckDeadline
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

## Compatibility Endpoints (Partial)

Gloci now also accepts a subset of Google Cloud-style endpoint shapes.

Pub/Sub topic create:

```bash
curl -s -X PUT http://localhost:8080/v1/projects/local/topics/my-topic
```

Pub/Sub publish:

```bash
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"data":"hello"}]}' \
  http://localhost:8080/v1/projects/local/topics/my-topic:publish
```

Pub/Sub subscription create:

```bash
curl -s -X PUT \
  -H 'Content-Type: application/json' \
  -d '{"topic":"projects/local/topics/my-topic"}' \
  http://localhost:8080/v1/projects/local/subscriptions/my-sub
```

Pub/Sub pull:

```bash
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"maxMessages":10}' \
  http://localhost:8080/v1/projects/local/subscriptions/my-sub:pull
```

Pub/Sub acknowledge:

```bash
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"ackIds":["my-sub:1"]}' \
  http://localhost:8080/v1/projects/local/subscriptions/my-sub:acknowledge
```

Pub/Sub modify ack deadline:

```bash
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"ackIds":["my-sub:1"],"ackDeadlineSeconds":30}' \
  http://localhost:8080/v1/projects/local/subscriptions/my-sub:modifyAckDeadline
```

Storage bucket create (JSON API style):

```bash
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"name":"my-bucket"}' \
  'http://localhost:8080/storage/v1/b?project=local'
```

Storage upload (simple upload style):

```bash
curl -s -X POST --data 'hello world' \
  'http://localhost:8080/upload/storage/v1/b/my-bucket/o?uploadType=media&name=greeting.txt'
```

Storage download:

```bash
curl -s \
  http://localhost:8080/download/storage/v1/b/my-bucket/o/greeting.txt
```

## Notes
- If `GLOCI_DATA_FILE` is not set, data is in-memory only and restart clears all resources.
- If `GLOCI_DATA_FILE` is set, Storage data and Pub/Sub queued/in-flight state are restored on startup.
- Pub/Sub pull delivers messages to in-flight state; unacked messages are redelivered after `ackDeadlineSeconds` (default: 10).
- Google Cloud compatibility is still partial; only common endpoint shapes are covered.
