"""Internal ASGI readiness, without a declared worker API."""

import asyncio

from main import health


def test_internal_health():
    messages = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    asyncio.run(health({"type": "http", "path": "/healthz"}, receive, send))
    assert messages == [
        {"type": "http.response.start", "status": 204, "headers": []},
        {"type": "http.response.body", "body": b""},
    ]
