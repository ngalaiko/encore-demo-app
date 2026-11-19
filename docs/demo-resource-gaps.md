# Demo resource gaps

This is the original comparison. Custom metrics, Redis, and cron are now implemented; see the [README](../README.md).

The demo already has a public OpenAPI gateway, PostgreSQL, two Pub/Sub topics and two subscriptions, custom spans, and contextual logs ([jobs](../jobs/main.py), [worker](../worker/main.py), [gateway](../gateway/encore.gateway.json)). [`demo.check`](../demo.check) only calls `/healthz`; [`tests/e2e.py`](../tests/e2e.py) checks completion, payload integrity, retry attempts, and recovery after a SQL error. The v2 check runner scores each curl by exit code, not response semantics ([check.rs](../../../encoredev/v2-encore/crates/encore_cli/src/daemon/check.rs#L304)).

| Missing | Demo scenario and assertion | v2 source |
| --- | --- | --- |
| Custom metrics | Count processing attempts and record processing latency, with bounded labels such as mode and outcome. On an isolated run, assert two attempts and one retry for `retry-once`. | [Python telemetry fixture](../../../encoredev/v2-encore/tests/py/telemetry/encore/main.py) |
| Object storage | Have the worker write a small result artifact to a private bucket. Read it through the API, assert exact bytes, and assert 404 for a missing artifact. Keep the object key stable across retries. | [Python storage fixture](../../../encoredev/v2-encore/tests/conformance/storage/py/storage/main.py) |
| Redis | Cache completed status only; PostgreSQL stays authoritative. Check miss, hit, and fallback after eviction. | [Python storage fixture](../../../encoredev/v2-encore/tests/conformance/storage/py/storage/main.py) |
| Cron | Run a reconciliation job for queued jobs missed by the initial publish. Trigger it explicitly and verify a second run is harmless. Local Encore does not schedule cron automatically. | [Cron conformance](../../../encoredev/v2-encore/tests/conformance/cron/README.md) |
| Typed config and secrets | Verify a test overlay changes a worker setting; add a test secret only when a feature needs a credential. Never expose it in a response. | [Python config fixture](../../../encoredev/v2-encore/tests/conformance/config/py/configs/main.py), [Python instructions](../../../encoredev/v2-encore/py_llm_instructions.txt) |
| ClickHouse | Job latency analytics; defer unless testing experimental infrastructure is a goal. Requires the `clickhouse` app feature. | [Python instructions](../../../encoredev/v2-encore/py_llm_instructions.txt), [feature gate test](../../../encoredev/v2-encore/crates/encore_parser/tests/clickhouse_feature.rs) |

Request Context and traced service-to-service HTTP are also absent. They are separate coverage targets, not new backing infrastructure ([Python propagation fixture](../../../encoredev/v2-encore/tests/py/reqctx-propagation/)).

Run the existing normal round trip in CI alongside the boot check first. Then add metrics for observability coverage or a private bucket for a new infrastructure path. Use an isolated test environment: the demo's 128-job admission budget does not refill ([README](../README.md)). The v2 checkout is the reference; installed beta and staging support for additions were not verified.
