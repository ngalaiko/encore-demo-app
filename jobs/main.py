"""Job submission and completion confirmation over SQL and Pub/Sub."""

import hashlib

import json
import re

from urllib.parse import parse_qs

import encore
from encore import EncoreError, cron, logs, metrics, pubsub
from encore.storage import postgres, redis

jobs = postgres.Database.named("jobs")
status_cache = redis.Database("job-status")
job_submitted = pubsub.Topic("job-submitted")
job_completed = pubsub.Topic.named("job-completed")
reconcile = cron.Job(
    "reconcile-jobs", path="/internal/reconcile", title="Reconcile queued jobs", every="1h",
)
status_cache_hits = metrics.Counter("status_cache_hits")
status_cache_misses = metrics.Counter("status_cache_misses")
jobs_republished = metrics.Counter("jobs_republished")

ID = re.compile(r"[A-Za-z0-9_-]{1,64}")


async def confirm_completion(message: pubsub.Message[dict]):
    result = message.data
    updated = await jobs.exec(
        """UPDATE jobs SET completion_confirmed = true
        WHERE id = $1 AND payload = $2 AND status = 'completed' AND NOT completion_confirmed""",
        result["id"], result["payload"],
    )
    if updated.rows_affected:
        logs.info("completion confirmed", job_id=result["id"], message_id=message.id)


completion_subscription = pubsub.Subscription[dict](
    job_completed, "confirm-completion", handler=confirm_completion,
    max_retries=5, min_backoff="1s", max_backoff="10s",
)


async def read_job(receive):
    body = bytearray()
    while True:
        chunk = await receive()
        if chunk["type"] != "http.request":
            raise ValueError("disconnected")
        body.extend(chunk.get("body", b""))
        if len(body) > 16384:
            raise ValueError("body too large")
        if not chunk.get("more_body", False):
            break
    job = json.loads(body.decode("utf-8"))
    if isinstance(job, dict):
        job = {"mode": "normal", "delay_ms": 0, "log_count": 3, **job}
    if not (
        isinstance(job, dict)
        and set(job) == {"id", "payload", "mode", "delay_ms", "log_count"}
        and isinstance(job["id"], str) and ID.fullmatch(job["id"])
        and isinstance(job["payload"], str)
        and len(job["payload"].encode("utf-8")) <= 4096
        and "\x00" not in job["payload"]
        and job["mode"] in ("normal", "slow", "retry-once")
        and type(job["delay_ms"]) is int and 0 <= job["delay_ms"] <= 500
        and type(job["log_count"]) is int and 3 <= job["log_count"] <= 250
    ):
        raise ValueError("invalid job")
    return job


async def reconcile_jobs():
    republished = 0
    for _ in range(16):
        async with jobs.begin() as tx:
            job = await tx.query(
                """SELECT id, payload, mode, delay_ms, log_count FROM jobs
                WHERE status = 'queued'
                  AND created_at <= now() - interval '5 minutes'
                  AND (last_republished_at IS NULL OR last_republished_at <= now() - interval '1 hour')
                ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED""",
            ).first()
            if job is None:
                break
            await job_submitted.publish(job)
            await tx.exec("UPDATE jobs SET last_republished_at = now() WHERE id = $1", job["id"])
        republished += 1
        jobs_republished.increment()
        logs.info("job republished", job_id=job["id"])
    return republished


async def get_job(job_id):
    key = f"job:v1:{job_id}"
    try:
        cached = await status_cache.string.get(key)
        if cached is not None:
            value = json.loads(cached)
            status_cache_hits.increment()
            return value
    except (EncoreError, ValueError):
        logs.warning("job status cache read failed", job_id=job_id)
    status_cache_misses.increment()

    job = await jobs.query(
        "SELECT id, status, attempts, payload, completion_confirmed FROM jobs WHERE id = $1", job_id,
    ).first()
    if job is None:
        return None
    job["payload_sha256"] = hashlib.sha256(job.pop("payload").encode("utf-8")).hexdigest()
    if job["status"] == "completed" and job["completion_confirmed"]:
        try:
            await status_cache.string.set(key, json.dumps(job).encode(), ex=60)
        except EncoreError:
            logs.warning("job status cache write failed", job_id=job_id)
    return job


async def handle(scope, receive):
    method, path = scope["method"], scope["path"]
    if method == "GET" and path == "/healthz":
        return 204, None
    if method == "POST" and path == "/internal/reconcile":
        return 200, {"republished": await reconcile_jobs()}

    if method == "POST" and path == "/jobs":
        try:
            job = await read_job(receive)
        except (ValueError, UnicodeError):
            return 400, {"error": "invalid job"}
        try:
            admitted = await jobs.query(
                """WITH budget AS (
                    UPDATE admission_budget SET remaining = remaining - 1
                    WHERE id = 1 AND remaining > 0 RETURNING id
                )
                INSERT INTO jobs (id, payload, mode, delay_ms, log_count)
                SELECT $1, $2, $3, $4, $5 FROM budget RETURNING id""",
                job["id"], job["payload"], job["mode"], job["delay_ms"], job["log_count"],
            ).first()
        except postgres.DatabaseError as error:
            if error.code == postgres.ErrorCode.UNIQUE_VIOLATION:
                return 409, {"error": "job already exists"}
            raise
        if admitted is None:
            return 429, {"error": "job budget exhausted"}
        await job_submitted.publish(job)
        logs.info("job queued", job_id=job["id"], mode=job["mode"])
        return 202, {"id": job["id"]}
    if method == "GET" and path.startswith("/jobs/"):
        job_id = path.removeprefix("/jobs/")
        if not ID.fullmatch(job_id):
            return 400, {"error": "invalid id"}
        try:
            failure = parse_qs(scope.get("query_string", b"").decode("utf-8")).get("fail", [""])[0]
        except UnicodeError:
            return 400, {"error": "invalid fail"}
        if failure not in ("", "db"):
            return 400, {"error": "invalid fail"}
        if failure == "db":
            async with jobs.begin() as tx:
                await tx.exec("SELECT 1 / 0")
        job = await get_job(job_id)
        if job is None:
            return 404, {"error": "job not found"}
        return 200, job
    return 404, {"error": "not found"}


async def app(scope, receive, send):
    if scope["type"] == "lifespan":
        while True:
            event = await receive()
            if event["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif event["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return
    if scope["type"] != "http":
        return
    try:
        status, value = await handle(scope, receive)
    except Exception:
        logs.exception("request failed")
        status, value = 500, {"error": "internal error"}
    body = b"" if value is None else json.dumps(value).encode("utf-8")
    await send({
        "type": "http.response.start", "status": status,
        "headers": [(b"content-type", b"application/json"), (b"x-content-type-options", b"nosniff")],
    })
    await send({"type": "http.response.body", "body": body})


if __name__ == "__main__":
    encore.run(app)
