"""Text analysis and durable retries."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import main


def test_retry_stores_and_publishes_analysis_result(monkeypatch):
    publish = AsyncMock()
    monkeypatch.setattr(main, "job_completed", SimpleNamespace(publish=publish))
    text = "one,two\u00a0λ\n雪"
    asyncio.run(main.jobs.exec(
        """INSERT INTO jobs (id, input_text, mode, delay_ms, log_count)
        VALUES ('retry-analysis', $1, 'retry-once', 0, 3)""", text,
    ))
    message = SimpleNamespace(id="submission", data={"id": "retry-analysis", "job_text": text})
    with pytest.raises(RuntimeError, match="retry-once first delivery"):
        asyncio.run(main.process_job(message))
    publish.assert_not_awaited()

    asyncio.run(main.process_job(message))
    publish.assert_awaited_once_with({"id": "retry-analysis", "job_text": text, "word_count": 3})
