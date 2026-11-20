"""ASGI request tests, with on-demand infrastructure for resource tests."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import main

from main import app


def request(path, method="GET", body=b""):
    messages = []

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        messages.append(message)

    asyncio.run(app({
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": method, "scheme": "http", "path": path,
        "query_string": b"", "headers": [],
        "server": ("localhost", 80), "client": ("localhost", 12345),
    }, receive, send))
    start, *chunks = messages
    raw = b"".join(chunk.get("body", b"") for chunk in chunks)
    return start["status"], dict(start["headers"]), json.loads(raw) if raw else None


def test_health():
    status, _, body = request("/healthz")
    assert status == 204
    assert body is None


VALID = {"text": "hello λ"}


@pytest.mark.parametrize("change", [
    {"text": None}, {"text": 42}, {"text": "λ" * 2049}, {"text": "\x00"},
    {"text": "\ud800"}, {"id": "caller-id"}, {"mode": "normal"}, {"delay_ms": 0},
    {"log_count": 3}, {"extra": True},
])
def test_invalid_jobs(change):
    body = json.dumps({**VALID, **change}).encode()
    assert request("/jobs", "POST", body)[0] == 400


@pytest.mark.parametrize("body", [b"", b"{", b"null", b"[]", b"{}", b"\xff", b"{} {}", b" " * 16385])
def test_invalid_json(body):
    assert request("/jobs", "POST", body)[0] == 400


@pytest.mark.parametrize("path,options", [
    ("/jobs", {}),
    ("/internal/demo/jobs", {"mode": "slow", "delay_ms": 50, "log_count": 5}),
    ("/internal/demo/jobs", {"mode": "retry-once"}),
])
def test_submit_publishes_job_text(monkeypatch, path, options):
    query = Mock(return_value=SimpleNamespace(first=AsyncMock(return_value={"id": "stored"})))
    publish = AsyncMock()
    monkeypatch.setattr(main, "jobs", SimpleNamespace(query=query))
    monkeypatch.setattr(main, "job_submitted", SimpleNamespace(publish=publish))

    status, _, body = request(path, "POST", json.dumps({**VALID, **options}).encode())
    assert status == 202
    assert set(body) == {"id"} and len(body["id"]) == 32
    assert query.call_args.args[1:3] == (body["id"], VALID["text"])
    publish.assert_awaited_once_with({
        "id": body["id"], "job_text": "hello λ", "mode": "normal",
        "delay_ms": 0, "log_count": 3, **options,
    })


def test_invalid_read_id():
    assert request("/jobs/bad id")[0] == 400


def test_unknown_route():
    assert request("/unknown")[0] == 404


@pytest.mark.parametrize("options", [
    {"mode": None}, {"mode": "unknown"}, {"delay_ms": -1}, {"delay_ms": 501},
    {"delay_ms": True}, {"delay_ms": None}, {"log_count": 2}, {"log_count": 251},
    {"log_count": 3.0}, {"log_count": None},
])
def test_invalid_demo_options(options):
    body = json.dumps({**VALID, **options}).encode()
    assert request("/internal/demo/jobs", "POST", body)[0] == 400


@pytest.mark.parametrize("path,method", [
    ("/probe/run", "GET"), ("/probe/jobs", "POST"),
    ("/probe/jobs/example", "GET"), ("/probe/db-error", "GET"),
    ("/demo/errors/database", "GET"), ("/demo/errors/database", "POST"),
    ("/demo/ping", "GET"),
])
def test_removed_routes(path, method):
    assert request(path, method)[0] == 404


@pytest.mark.parametrize("status", ["queued", "processing"])
def test_pending_job_has_no_result(monkeypatch, status):
    row = {"id": "example", "status": status, "word_count": None, "completion_confirmed": False}
    query = Mock(return_value=SimpleNamespace(first=AsyncMock(return_value=row)))
    monkeypatch.setattr(main, "jobs", SimpleNamespace(query=query))
    cache = SimpleNamespace(get=AsyncMock(return_value=None), set=AsyncMock())
    monkeypatch.setattr(main, "status_cache", SimpleNamespace(string=cache))

    assert request("/jobs/example")[2] == {"id": "example", "status": status}
    cache.set.assert_not_awaited()


def test_completed_status_cache(monkeypatch):
    row = {
        "id": "example", "status": "completed", "word_count": 0, "completion_confirmed": True,
    }
    query = Mock(return_value=SimpleNamespace(first=AsyncMock(side_effect=lambda: row.copy())))
    begin = Mock()
    begin.return_value.__aenter__ = AsyncMock(return_value=SimpleNamespace(
        exec=AsyncMock(side_effect=main.EncoreError("database failure")),
    ))
    begin.return_value.__aexit__ = AsyncMock(return_value=False)
    monkeypatch.setattr(main, "jobs", SimpleNamespace(query=query, begin=begin))
    cache = SimpleNamespace(get=AsyncMock(return_value=None), set=AsyncMock())
    monkeypatch.setattr(main, "status_cache", SimpleNamespace(string=cache))

    status, _, body = request("/jobs/example")
    assert status == 200
    assert body == {"id": "example", "status": "completed", "result": {"word_count": 0}}
    assert cache.set.call_args.kwargs == {"ex": 60}

    cache.get.return_value = cache.set.call_args.args[1]
    assert request("/jobs/example")[2] == body
    assert query.call_count == 1
    assert request("/internal/demo/database-error", "POST")[0] == 500
    begin.assert_called_once()
    assert cache.get.await_count == 2

    cache.get.side_effect = main.EncoreError("redis unavailable")
    cache.set.side_effect = main.EncoreError("redis unavailable")
    assert request("/jobs/example")[2] == body
    assert query.call_count == 2


def test_status_is_cached_after_matching_completion():
    asyncio.run(main.jobs.exec(
        """INSERT INTO jobs (id, input_text, mode, delay_ms, log_count, status, word_count)
        VALUES ('confirmed', 'hello', 'normal', 0, 3, 'completed', 1)""",
    ))

    def complete(word_count):
        asyncio.run(main.confirm_completion(SimpleNamespace(
            id="completion", data={"id": "confirmed", "job_text": "hello", "word_count": word_count},
        )))

    complete(2)
    assert request("/jobs/confirmed")[0] == 200
    assert asyncio.run(main.status_cache.string.get("job:v2:confirmed")) is None
    complete(1)
    status, _, body = request("/jobs/confirmed")
    assert status == 200
    cached = asyncio.run(main.status_cache.string.get("job:v2:confirmed"))
    assert cached is not None and json.loads(cached) == body
    assert body == {"id": "confirmed", "status": "completed", "result": {"word_count": 1}}


def test_reconcile_stale_job(monkeypatch):
    publish = AsyncMock()
    monkeypatch.setattr(main, "job_submitted", SimpleNamespace(publish=publish))

    async def seed():
        await main.jobs.exec(
            """INSERT INTO jobs (id, input_text, mode, delay_ms, log_count, created_at)
            VALUES ('stale', 'hello', 'normal', 0, 3, now() - interval '10 minutes'),
                   ('fresh', 'hello', 'normal', 0, 3, now())""",
        )

    asyncio.run(seed())
    assert request("/internal/reconcile", "POST")[2] == {"republished": 1}
    publish.assert_awaited_once_with({"id": "stale", "job_text": "hello", "mode": "normal", "delay_ms": 0, "log_count": 3})
    assert request("/internal/reconcile", "POST")[2] == {"republished": 0}
    assert publish.await_count == 1
