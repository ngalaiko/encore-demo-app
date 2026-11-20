#!/usr/bin/env python3
"""Exercise the owned DEV app through its public HTTP API."""

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request


DEV_HOST = "staging-encore-demo-app-7bm2.encr.app"


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


def successful_round_trip(base):
    code, body = request(base, "/jobs", "POST", {"text": "one two\nλ\u00a0雪"})
    assert code == 202, f"submit: expected HTTP202, got {code}: {body!r}"
    job_id = body["id"]
    assert len(job_id) == 32 and set(body) == {"id"}, f"submit identity: {body!r}"
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        code, body = request(base, "/jobs/" + job_id)
        assert code == 200, f"read: expected HTTP200, got {code}: {body!r}"
        assert body["id"] == job_id, f"read identity: {body!r}"
        if body["status"] == "completed":
            assert body == {"id": job_id, "status": "completed", "result": {"word_count": 4}}, body
            print(json.dumps({"case": "successful_round_trip", "result": "pass", "id": job_id}))
            return
        assert set(body) == {"id", "status"} and body["status"] in ("queued", "processing"), body
        time.sleep(1)
    raise AssertionError(f"job completion timed out: {body!r}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True, type=checked_base)
    args = parser.parse_args()
    successful_round_trip(args.base_url)


if __name__ == "__main__":
    main()
