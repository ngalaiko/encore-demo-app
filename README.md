# Encore demo app

A Python background job demo built with Encore. Submit a job, process it through
Pub/Sub and PostgreSQL, then inspect its status. Normal, slow and retry scenarios
generate traces, contextual logs and database activity.

## Structure

- `jobs`: HTTP, Redis status cache, and reconciliation cron; publishes `job-submitted` and consumes `job-completed`.
- `worker`: consumes `job-submitted`; owns PostgreSQL `jobs` and `job-completed`.
- `gateway`: default public gateway `api`, required by the exposed routes.

Subscriptions are `process-job` and `confirm-completion`. Both Python modules
use `main.py` as their entrypoint. The worker has an internal readiness listener
but no public endpoints.

## Routes

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/healthz` | Health check; returns 204 |

| POST | `/jobs` | Store and publish a job; returns 202 with its ID |
| GET | `/jobs/{id}` | Status, attempts, payload SHA-256 and completion confirmation |
| GET | `/jobs/{id}?fail=db` | Deliberate SQL division by zero; rollback and 500 |

Minimal job:

```json
{"id":"example-1","payload":"hello"}
```

Optional fields default to `mode: "normal"`, `delay_ms: 0` and `log_count: 3`.
Explicit nulls and unknown fields are rejected.

- `normal`: complete normally.
- `slow`: run `pg_sleep` for `delay_ms`.
- `retry-once`: fail after committing the first attempt, then complete on retry.

IDs use 1–64 letters, digits, `_` or `-`. JSON is limited to 16KiB; payload to
4096 UTF-8 bytes without NUL; delay to 0–500ms; log count to 3–250.
Each environment admits 128 jobs, without refill. Duplicates return 409;
exhausted budget returns 429. Publishing follows SQL admission; a publishing
failure returns 500 and can leave a queued job for reconciliation.

Status progresses from `queued` through `processing` to `completed`.
`completion_confirmed` means the completion message's full payload matched the
stored job. Public reads expose only the payload digest. Completed redeliveries
do not increment attempts and republish unconfirmed completions. Redis caches
only confirmed completions for 60 seconds, without the original payload.
Cache misses and Redis errors fall back to PostgreSQL. `?fail=db` bypasses Redis.

The hourly `reconcile-jobs` cron calls private `POST /internal/reconcile`.
It republishes up to 16 jobs queued for at least five minutes, with a one-hour
cooldown per republished job. It does not admit new jobs or spend the budget.
Publication is at-least-once. Locally, cron is not scheduled; use
`encore-beta cron trigger reconcile-jobs` with a CLI that supports cron.

The worker emits `log_count` contextual logs inside a `process_job` span, including
false, zero, empty-string and large-integer attributes.

Custom metrics include `worker.job_attempts`, `worker.job_retries` (deliberate
retry failures), `jobs.status_cache_hits`, `jobs.status_cache_misses`, and
`jobs.jobs_republished`. After a local `retry-once` run, inspect them with
`encore-beta metrics get worker.job_attempts --since 5m` and
`encore-beta metrics get worker.job_retries --since 5m`.

## Trace redaction

Submit a sample job to a running gateway:

```sh
curl -i http://localhost:4001/jobs \
  -H 'Content-Type: application/json' \
  -H 'X-Encore-Force-Trace-Sample: true' \
  -d '{"id":"redaction-demo-1","payload":"example private text"}'
```

Use a new ID each time. In Trace Explorer, open the HTTP request body, then
follow the Pub/Sub publish spans and their delivery links. The job ID stays
visible; `payload` appears as `[REDACTED]` on the HTTP request and both topics.
The OpenAPI schema marks the HTTP field as sensitive; the Python topic schemas
use `Annotated[str, Sensitive()]`. Only telemetry is redacted: storage and
subscribers still receive the original value.

## Checks

Use `encore-beta` with an SDK/runtime pair supporting `encore-dev>=2.0.0b8`:

```sh
encore-beta test uv run --project jobs python -m pytest jobs/test_http.py -q
encore-beta test uv run --project worker python -m pytest worker/test_health.py -q
encore-beta check < demo.check
```

Against a running app's gateway:

```sh
python3 tests/e2e.py --base-url http://localhost:4001 --all-cases
```

The HTTP tests cover health, validation, defaults, cache fallback, and PostgreSQL-backed cron reconciliation.
The end-to-end test checks SQL and Pub/Sub completion; `--all-cases` adds slow,
retry and database-error scenarios. Its normal case submits only `id` and `payload`.

Migration `3_job_names.up.sql` converts stored `ok`/`done` values to
`normal`/`completed` and renames `result_received` to `completion_confirmed`.
The renamed routes, topics and subscriptions are breaking changes; drain queued
work before upgrading an existing environment.
