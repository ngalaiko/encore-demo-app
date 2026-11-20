# Encore text analysis demo

Submit private text for background analysis and poll for its word count. The
Python services exercise Encore's HTTP gateway, Pub/Sub, PostgreSQL, Redis, cron,
and tracing.

## Public API

| Method | Path | Response |
| --- | --- | --- |
| GET | `/healthz` | 204 |
| POST | `/jobs` | 202 with a server-generated job ID |
| GET | `/jobs/{id}` | Job status and, once completed, its result |

Submit `{"text":"example private text"}`, then poll using the returned ID.
A completed response looks like:

```json
{"id":"88f43c9c9f704457952716093c13efe8a","status":"completed","result":{"word_count":3}}
```

Queued and processing jobs omit `result`. A word is a run of characters
separated by Unicode whitespace; punctuation does not split words. Empty or
whitespace-only text yields zero. Text is limited to 4096 UTF-8 bytes without
NUL, and the JSON request is limited to 16KiB. Responses never include the
original text.

Each environment admits 128 jobs without refill, then returns 429. A publish
failure can leave an admitted job queued for reconciliation.

## Processing and demo controls

The `jobs` service publishes `job-submitted`; the `worker` service consumes
it through `process-job` and stores the result in PostgreSQL. The worker
publishes `job-completed` until `confirm-completion` confirms the result.
Redis caches only confirmed results for 60 seconds. Cache misses and Redis
errors fall back to PostgreSQL.

The hourly `reconcile-jobs` cron invokes private `POST /internal/reconcile`.
It republishes up to 16 jobs queued for at least five minutes, with a one-hour
cooldown. Local cron is not scheduled; use `encore-beta cron trigger reconcile-jobs`
with a CLI that supports cron.

Private `POST /internal/demo/jobs` accepts `text` and optional controls:

- `mode`: `normal` (default), `slow` (runs `pg_sleep`), or `retry-once` (fails after committing the first attempt).
- `delay_ms`: 0–500, default 0.
- `log_count`: 3–250, default 3.

Private `POST /internal/demo/database-error` runs a failing SQL transaction.
Neither route is exposed at the public gateway.

The worker emits contextual logs in a `process_job` span. Inspect
`worker.job_attempts`, `worker.job_retries`, `jobs.status_cache_hits`,
`jobs.status_cache_misses`, and `jobs.jobs_republished` with
`encore-beta metrics get <name> --since 5m`.

## Trace redaction

```sh
curl -i http://localhost:4001/jobs \
  -H 'Content-Type: application/json' \
  -H 'X-Encore-Force-Trace-Sample: true' \
  -d '{"text":"example private text"}'
```

In Trace Explorer, follow the request body and Pub/Sub delivery links.
The HTTP `text` field and both topics' `job_text` fields appear as
`[REDACTED]`. The job ID and result stay visible. Storage and subscribers
still receive the original text.

## Checks

Use a matching Encore SDK and runtime supporting `encore-dev>=2.0.0b8`:

```sh
encore-beta test uv run --project jobs python -m pytest jobs/test_http.py -q
encore-beta test uv run --project worker python -m pytest worker -q
encore-beta check < demo.check
python3 tests/e2e.py --base-url http://localhost:4001
```

## Upgrading

The public request and response schemas and the completion message changed.
Drain queued and in-flight messages before upgrading. Migration
`5_text_analysis.up.sql` retains job IDs and backfills completed results.
