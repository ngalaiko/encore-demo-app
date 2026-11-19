"""ASGI request tests, with on-demand infrastructure for resource tests."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import main

from main import app, read_job


def request(path, method="GET", body=b"", query=b""):
    messages = []

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        messages.append(message)

    asyncio.run(app({
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": method, "scheme": "http", "path": path,
        "query_string": query, "headers": [],
        "server": ("localhost", 80), "client": ("localhost", 12345),
    }, receive, send))
    start, *chunks = messages
    raw = b"".join(chunk.get("body", b"") for chunk in chunks)
    return start["status"], dict(start["headers"]), json.loads(raw) if raw else None


def test_health():
    status, _, body = request("/healthz")
    assert status == 204
    assert body is None


VALID = {"id": "test-1", "payload": "hello λ", "mode": "normal", "delay_ms": 0, "log_count": 3}


@pytest.mark.parametrize("change", [
    {"id": "bad id"}, {"id": "x" * 65}, {"payload": None},
    {"payload": "λ" * 2049}, {"payload": "\x00"}, {"payload": "\ud800"},
    {"mode": "unknown"}, {"mode": "ok"}, {"delay_ms": -1}, {"delay_ms": 501},
    {"delay_ms": True}, {"log_count": 2}, {"log_count": 251},
    {"log_count": 3.0}, {"extra": True},
])
def test_invalid_jobs(change):
    body = json.dumps({**VALID, **change}).encode()
    assert request("/jobs", "POST", body)[0] == 400


@pytest.mark.parametrize("body", [b"", b"{", b"null", b"[]", b"{}", b"\xff", b"{} {}", b" " * 16385])
def test_invalid_json(body):
    assert request("/jobs", "POST", body)[0] == 400


@pytest.mark.parametrize("field", ["id", "payload"])
def test_missing_job_field(field):
    body = dict(VALID)
    del body[field]
    assert request("/jobs", "POST", json.dumps(body).encode())[0] == 400


def test_invalid_read_id():
    assert request("/jobs/bad id")[0] == 400


def test_unknown_route():
    assert request("/unknown")[0] == 404


@pytest.mark.parametrize("options", [
    {}, {"mode": "slow"}, {"delay_ms": 100}, {"log_count": 10},
    {"mode": "retry-once", "delay_ms": 50, "log_count": 5},
])
def test_job_defaults(options):
    body = {"id": "minimal-1", "payload": "hello", **options}

    async def receive():
        return {"type": "http.request", "body": json.dumps(body).encode()}

    assert asyncio.run(read_job(receive)) == {
        "id": "minimal-1", "payload": "hello",
        "mode": "normal", "delay_ms": 0, "log_count": 3, **options,
    }


@pytest.mark.parametrize("field", ["mode", "delay_ms", "log_count"])
def test_null_options_rejected(field):
    body = {"id": "minimal-1", "payload": "hello", field: None}
    assert request("/jobs", "POST", json.dumps(body).encode())[0] == 400


@pytest.mark.parametrize("path,method", [
    ("/probe/run", "GET"), ("/probe/jobs", "POST"),
    ("/probe/jobs/example", "GET"), ("/probe/db-error", "GET"),
    ("/demo/errors/database", "GET"), ("/demo/errors/database", "POST"),
    ("/demo/ping", "GET"),
])
def test_removed_routes(path, method):
    assert request(path, method)[0] == 404


@pytest.mark.parametrize("query", [b"fail=unknown", b"fail=\xff"])
def test_invalid_failure(query):
    assert request("/jobs/example", query=query)[0] == 400


def test_completed_status_cache(monkeypatch):
    row = {
        "id": "example", "status": "completed", "attempts": 1,
        "payload": "hello", "completion_confirmed": True,
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
    assert status == 200 and "payload" not in body
    assert body["payload_sha256"] == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    assert cache.set.call_args.kwargs == {"ex": 60}

    cache.get.return_value = cache.set.call_args.args[1]
    assert request("/jobs/example")[2] == body
    assert query.call_count == 1
    assert request("/jobs/example", query=b"fail=db")[0] == 500
    begin.assert_called_once()
    assert cache.get.await_count == 2

    cache.get.side_effect = main.EncoreError("redis unavailable")
    cache.set.side_effect = main.EncoreError("redis unavailable")
    assert request("/jobs/example")[2] == body
    assert query.call_count == 2


def test_confirmed_status_is_cached_in_redis():
    asyncio.run(main.jobs.exec(
        """INSERT INTO jobs (id, payload, mode, delay_ms, log_count, status, attempts, completion_confirmed)
        VALUES ('confirmed', 'hello', 'normal', 0, 3, 'completed', 1, true)""",
    ))
    status, _, body = request("/jobs/confirmed")
    assert status == 200
    cached = asyncio.run(main.status_cache.string.get("job:v1:confirmed"))
    assert cached is not None and json.loads(cached) == body
    assert "payload" not in body


def test_reconcile_stale_job(monkeypatch):
    publish = AsyncMock()
    monkeypatch.setattr(main, "job_submitted", SimpleNamespace(publish=publish))

    async def seed():
        await main.jobs.exec(
            """INSERT INTO jobs (id, payload, mode, delay_ms, log_count, created_at)
            VALUES ('stale', 'hello', 'normal', 0, 3, now() - interval '10 minutes'),
                   ('fresh', 'hello', 'normal', 0, 3, now())""",
        )

    asyncio.run(seed())
    assert request("/internal/reconcile", "POST")[2] == {"republished": 1}
    publish.assert_awaited_once_with({"id": "stale", "payload": "hello", "mode": "normal", "delay_ms": 0, "log_count": 3})
    assert request("/internal/reconcile", "POST")[2] == {"republished": 0}
    assert publish.await_count == 1
