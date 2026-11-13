"""Public ASGI requests that do not need provisioned resources."""

import asyncio
import json

import pytest

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
