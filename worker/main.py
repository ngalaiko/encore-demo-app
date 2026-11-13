"""API-less worker producing real SQL, retry and result-delivery traces."""

import encore
from encore import logs, pubsub, tracing
from encore.storage import postgres

jobs = postgres.Database("jobs", migrations="./migrations")
job_completed = pubsub.Topic("job-completed")
job_submitted = pubsub.Topic.named("job-submitted")


async def process_job(message: pubsub.Message[dict]):
    data = message.data
    job_id = data["id"]
    with tracing.span("process_job", attributes={
        "job.id": job_id, "test.flag": False, "test.zero": 0,
        "test.empty": "", "test.int64": 9007199254740993,
    }) as span:
        # Commit the attempt before a deliberate retry can fail the delivery.
        async with jobs.begin() as tx:
            job = await tx.query(
                "SELECT * FROM jobs WHERE id = $1 AND payload = $2 FOR UPDATE",
                job_id, data["payload"],
            ).one()
            if job["status"] != "completed":
                job = await tx.query(
                    """UPDATE jobs SET attempts = attempts + 1, status = 'processing'
                    WHERE id = $1 RETURNING *""", job_id,
                ).one()
        span.set_attribute("job.attempt", job["attempts"])
        if job["status"] != "completed":
            if job["mode"] == "retry-once" and job["attempts"] == 1:
                logs.warning("retrying job", job_id=job_id, attempt=job["attempts"], message_id=message.id)
                raise RuntimeError("retry-once first delivery")
            async with jobs.begin() as tx:
                job = await tx.query("SELECT * FROM jobs WHERE id = $1 FOR UPDATE", job_id).one()
                if job["status"] != "completed":
                    if job["mode"] == "slow":
                        await tx.exec("SELECT pg_sleep($1::double precision / 1000.0)", job["delay_ms"])
                    for index in range(job["log_count"]):
                        logs.info(
                            "processing job", job_id=job_id, mode=job["mode"],
                            attempt=job["attempts"], step=index + 1, total=job["log_count"],
                            message_id=message.id,
                        )
                    await tx.exec("UPDATE jobs SET status = 'completed' WHERE id = $1", job_id)
        # Completed redeliveries republish until the API confirms the payload.
        if not job["completion_confirmed"]:
            await job_completed.publish({"id": job_id, "payload": job["payload"]})
        logs.info("job completed", job_id=job_id, attempt=job["attempts"])


job_subscription = pubsub.Subscription[dict](
    job_submitted, "process-job", handler=process_job,
    max_retries=5, min_backoff="1s", max_backoff="10s",
)


async def health(scope, receive, send):
    """Internal readiness listener; no declared worker API."""
    if scope["type"] == "lifespan":
        while True:
            event = await receive()
            if event["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif event["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return
    if scope["type"] == "http":
        status = 204 if scope["path"] == "/healthz" else 404
        await send({"type": "http.response.start", "status": status, "headers": []})
        await send({"type": "http.response.body", "body": b""})


if __name__ == "__main__":
    encore.run(health)
