# Gloci Initial Implementation Plan

## Scope (from SPEC.md)
- Implement a GCP emulator in Nim.
- Support local emulation for:
  - Cloud Storage
  - Pub/Sub
  - Cloud Scheduler
- Planned extension:
  - BigQuery
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
- [x] Add persistence option for Storage and queued Pub/Sub messages.
- [x] Add ack/ackDeadline semantics to Pub/Sub pull flow.
- [x] Add cron expression parsing for Scheduler.
- [ ] Add integration tests (curl + expected JSON).

## Phase 3 (stabilization)
- [ ] Add structured logging and request tracing.
- [ ] Add config file support.
- [ ] Add CI for build and tests.

## Phase 4 (BigQuery MVP)
- [x] Add in-memory BigQuery resources:
  - Project namespace (reuse existing `projectId` handling pattern).
  - Dataset metadata (`datasetId`, `location`, labels, createdAt).
  - Table metadata (`tableId`, schema fields, createdAt, updatedAt).
  - Rows store per table.
- [x] Add BigQuery JSON API baseline endpoints:
  - `GET/POST /bigquery/v2/projects/{projectId}/datasets` (list/insert).
  - `GET/DELETE /bigquery/v2/projects/{projectId}/datasets/{datasetId}`.
  - `GET/POST /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables` (list/insert).
  - `GET/DELETE /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}`.
  - `POST /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/insertAll`.
  - `GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/data` (simple paging).
- [x] Add validation rules:
  - Dataset/table naming constraints.
  - Schema type checks on `insertAll`.
  - Duplicate row handling with optional `insertId` de-dup window.
- [x] Add persistence for BigQuery metadata and rows through `GLOCI_DATA_FILE`.

## Phase 5 (BigQuery Query & Jobs)
- [ ] Add query execution MVP:
  - `POST /bigquery/v2/projects/{projectId}/queries` (sync query).
  - Minimal SQL subset: `SELECT`, `WHERE`, `LIMIT`, projection, simple aggregates.
  - Document explicit non-goals for unsupported SQL (JOIN, nested/repeated, DDL in first iteration).
- [ ] Add job lifecycle compatibility:
  - `POST /bigquery/v2/projects/{projectId}/jobs` (query jobs).
  - `GET /bigquery/v2/projects/{projectId}/jobs/{jobId}`.
  - `GET /bigquery/v2/projects/{projectId}/queries/{jobId}` (query results).
- [ ] Add async job state model (`PENDING`/`RUNNING`/`DONE`) with deterministic local timing.

## Phase 6 (BigQuery Stabilization)
- [ ] Add BigQuery integration tests (curl + expected JSON + query snapshots).
- [ ] Add emulator compatibility fixtures against real BigQuery response shapes for key endpoints.
- [ ] Add scale guardrails:
  - configurable max rows per table,
  - max response size,
  - predictable pagination tokens.
- [ ] Add observability:
  - structured request logs with project/dataset/table/job identifiers,
  - query execution metrics (duration, scanned rows).
