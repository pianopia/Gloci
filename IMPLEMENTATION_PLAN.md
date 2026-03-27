# Gloci Initial Implementation Plan

## Scope (from SPEC.md)
- Implement a GCP emulator in Nim.
- Support local emulation for:
  - Cloud Storage
  - Pub/Sub
  - Cloud Scheduler
- Run with Docker Compose.

## Phase 1 (MVP: now)
- [x] Create a Nim HTTP server.
- [x] Add in-memory resources for Storage / PubSub / Scheduler.
- [x] Implement basic APIs:
  - Storage: bucket create/list, object put/get/list
  - Pub/Sub: topic create/list, subscription create/list, publish/pull
  - Scheduler: job create/list, manual run, interval execution (`everySeconds`)
- [x] Add `Dockerfile` and `docker-compose.yml`.
- [x] Add usage docs (`README.md`).

## Phase 2 (next)
- [x] Improve API compatibility with official Google Cloud emulator-style endpoints.
  - Added partial compatibility routes for Pub/Sub (`/v1/projects/...`) and Storage JSON API style (`/storage/v1/b`, `/upload/storage/v1/b/...`, `/download/storage/v1/b/...`).
- [ ] Add persistence option for Storage and queued Pub/Sub messages.
- [ ] Add ack/ackDeadline semantics to Pub/Sub pull flow.
- [ ] Add cron expression parsing for Scheduler.
- [ ] Add integration tests (curl + expected JSON).

## Phase 3 (stabilization)
- [ ] Add structured logging and request tracing.
- [ ] Add config file support.
- [ ] Add CI for build and tests.
