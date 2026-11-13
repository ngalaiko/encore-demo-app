#!/usr/bin/env python3
"""Exercise the owned DEV app through its public HTTP API."""

import argparse
import hashlib
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


DEV_HOST = "staging-span-explorer-e2e-1112-8mfi.staging.encr.app"


def checked_base(value):
    url = urllib.parse.urlsplit(value)
    local = url.scheme == "http" and url.hostname in ("localhost", "127.0.0.1")
    owned = url.scheme == "https" and url.hostname == DEV_HOST and url.port in (None, 443)
    if not (local or owned) or url.username or url.password or url.query or url.fragment or url.path not in ("", "/"):
        raise argparse.ArgumentTypeError("use the owned DEV app or a loopback server")
    return value.rstrip("/")


def request(base, path, method="GET", body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"User-Agent": "encore-demo-e2e/1.0"},
    )
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        response = urllib.request.urlopen(req, timeout=30)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read(65537)
        if len(raw) > 65536:
            raise AssertionError("response exceeds 64KiB")
        try:
            value = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            value = raw.decode("utf-8", errors="replace")[:256]
        return response.status, value


def successful_round_trip(base, mode="normal"):
    job_id = "e2e-" + uuid.uuid4().hex
    payload = "line one\nUnicode: λ 雪\nempty: \"\"; null: null"
    job = {
        "id": job_id,
        "payload": payload,
        "mode": mode,
        "delay_ms": 500 if mode == "slow" else 0,
        "log_count": 250 if mode == "slow" else 3,
    }
    if mode == "normal":
        job = {"id": job_id, "payload": payload}
    code, body = request(base, "/jobs", "POST", job)
    assert code == 202, f"submit: expected HTTP202, got {code}: {body!r}"
    assert body["id"] == job_id, f"submit identity: {body!r}"
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        code, body = request(base, "/jobs/" + job_id)
        assert code == 200, f"read: expected HTTP200, got {code}: {body!r}"
        assert body["id"] == job_id, f"read identity: {body!r}"
        assert "payload" not in body, f"public response exposes payload: {body!r}"
        assert body["payload_sha256"] == hashlib.sha256(payload.encode()).hexdigest(), f"payload digest: {body!r}"
        if body["status"] == "completed" and body["completion_confirmed"]:
            expected_attempts = 2 if mode == "retry-once" else 1
            assert body["attempts"] == expected_attempts, f"delivery attempts: {body!r}"
            print(json.dumps({"case": "successful_round_trip", "mode": mode, "result": "pass", "id": job_id}))
            return job_id
        assert body["status"] in ("queued", "processing", "completed"), f"job state: {body!r}"
        time.sleep(1)
    raise AssertionError(f"job completion timed out: {body!r}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True, type=checked_base)
    parser.add_argument("--all-cases", action="store_true", help="also exercise slow, retry-once and SQL failure")
    args = parser.parse_args()
    completed_id = successful_round_trip(args.base_url)
    if args.all_cases:
        successful_round_trip(args.base_url, "slow")
        successful_round_trip(args.base_url, "retry-once")
        code, body = request(args.base_url, "/jobs/" + completed_id + "?fail=db")
        assert code == 500 and body == {"error": "internal error"}, f"SQL failure: {code}: {body!r}"
        code, body = request(args.base_url, "/jobs/" + completed_id)
        assert code == 200 and body["status"] == "completed" and body["completion_confirmed"], f"read after rollback: {code}: {body!r}"
        print(json.dumps({"case": "database_error", "result": "pass"}))


if __name__ == "__main__":
    main()
