"""Job submission and completion confirmation over SQL and Pub/Sub."""

import json
import re
import uuid
from typing import Annotated, TypedDict

import encore
from encore import EncoreError, Sensitive, cron, logs, metrics, pubsub
from encore.storage import postgres, redis

class JobSubmitted(TypedDict):
    id: str
    job_text: Annotated[str, Sensitive()]
    mode: str
    delay_ms: int
    log_count: int


jobs = postgres.Database.named("jobs")
status_cache = redis.Database("job-status")
job_submitted = pubsub.Topic[JobSubmitted]("job-submitted")
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
        WHERE id = $1 AND input_text = $2 AND word_count = $3
          AND status = 'completed' AND NOT completion_confirmed""",
        result["id"], result["job_text"], result["word_count"],
    )
    if updated.rows_affected:
        logs.info("completion confirmed", job_id=result["id"], message_id=message.id)


completion_subscription = pubsub.Subscription[dict](
    job_completed, "confirm-completion", handler=confirm_completion,
    max_retries=5, min_backoff="1s", max_backoff="10s",
)


async def read_submission(receive, *, demo=False):
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
    submission = json.loads(body.decode("utf-8"))
    options = {"mode": "normal", "delay_ms": 0, "log_count": 3}
    allowed_fields = {"text", *options} if demo else {"text"}
    if not isinstance(submission, dict) or submission.keys() - allowed_fields:
        raise ValueError("invalid submission")
    text = submission.get("text")
    if not isinstance(text, str) or len(text.encode("utf-8")) > 4096 or "\x00" in text:
        raise ValueError("invalid text")
    job = {**options, **submission}
    if not (
        job["mode"] in ("normal", "slow", "retry-once")
        and type(job["delay_ms"]) is int and 0 <= job["delay_ms"] <= 500
        and type(job["log_count"]) is int and 3 <= job["log_count"] <= 250
    ):
        raise ValueError("invalid demo options")
    return job


async def reconcile_jobs():
    republished = 0
    for _ in range(16):
        async with jobs.begin() as tx:
            job = await tx.query(
                """SELECT id, input_text AS job_text, mode, delay_ms, log_count FROM jobs
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
    key = f"job:v2:{job_id}"
    try:
        cached = await status_cache.string.get(key)
        if cached is not None:
            value = json.loads(cached)
            status_cache_hits.increment()
            return value
    except (EncoreError, ValueError):
        logs.warning("job status cache read failed", job_id=job_id)
    status_cache_misses.increment()

    row = await jobs.query(
        "SELECT id, status, word_count, completion_confirmed FROM jobs WHERE id = $1", job_id,
    ).first()
    if row is None:
        return None
    job = {"id": row["id"], "status": row["status"]}
    if row["status"] == "completed":
        job["result"] = {"word_count": row["word_count"]}
    if row["status"] == "completed" and row["completion_confirmed"]:
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

    if method == "POST" and path == "/internal/demo/database-error":
        async with jobs.begin() as tx:
            await tx.exec("SELECT 1 / 0")

    if method == "POST" and path in ("/jobs", "/internal/demo/jobs"):
        try:
            job = await read_submission(receive, demo=path == "/internal/demo/jobs")
        except ValueError:
            return 400, {"error": "invalid submission"}
        job_id = uuid.uuid4().hex
        admitted = await jobs.query(
            """WITH budget AS (
                UPDATE admission_budget SET remaining = remaining - 1
                WHERE id = 1 AND remaining > 0 RETURNING id
            )
            INSERT INTO jobs (id, input_text, mode, delay_ms, log_count)
            SELECT $1, $2, $3, $4, $5 FROM budget RETURNING id""",
            job_id, job["text"], job["mode"], job["delay_ms"], job["log_count"],
        ).first()
        if admitted is None:
            return 429, {"error": "job budget exhausted"}
        await job_submitted.publish({
            "id": job_id, "job_text": job["text"], "mode": job["mode"],
            "delay_ms": job["delay_ms"], "log_count": job["log_count"],
        })
        logs.info("job queued", job_id=job_id, mode=job["mode"])
        return 202, {"id": job_id}
    if method == "GET" and path.startswith("/jobs/"):
        job_id = path.removeprefix("/jobs/")
        if not ID.fullmatch(job_id):
            return 400, {"error": "invalid id"}
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
