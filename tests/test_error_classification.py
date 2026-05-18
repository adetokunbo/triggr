from __future__ import annotations

import asyncio
import pytest

from triggers import (
    AllTransient,
    ErrorKind,
    Lifecycle,
    PollingLoop,
    TaskOutcome,
    TaskProcessor,
    RetryPolicy,
)


class FatalOnValueError:
    """Classifies ValueError as fatal, everything else as transient."""

    def classify(self, error: Exception) -> ErrorKind:
        if isinstance(error, ValueError):
            return ErrorKind.FATAL
        return ErrorKind.TRANSIENT


class AlwaysFailsWorker:
    def __init__(self, error: Exception) -> None:
        self._error = error
        self.attempts = 0

    async def complete_task(self, task: str) -> TaskOutcome:
        self.attempts += 1
        raise self._error

    async def is_stale_task(self, task: str) -> bool:
        return False


class TestErrorClassificationInProcessor:
    @pytest.mark.asyncio
    async def test_fatal_error_no_retry(self):
        policy = RetryPolicy(max_retries=5, initial_delay=0.001, max_delay=0.01)
        worker = AlwaysFailsWorker(ValueError("fatal"))
        proc = TaskProcessor(worker, policy, error_classifier=FatalOnValueError())

        result = await proc.process("task")

        assert result is False
        assert worker.attempts == 1

    @pytest.mark.asyncio
    async def test_transient_error_retries(self):
        policy = RetryPolicy(max_retries=3, initial_delay=0.001, max_delay=0.01)
        worker = AlwaysFailsWorker(RuntimeError("transient"))
        proc = TaskProcessor(worker, policy, error_classifier=FatalOnValueError())

        result = await proc.process("task")

        assert result is False
        assert worker.attempts == 4  # 1 initial + 3 retries

    @pytest.mark.asyncio
    async def test_default_classifier_retries_everything(self):
        policy = RetryPolicy(max_retries=2, initial_delay=0.001, max_delay=0.01)
        worker = AlwaysFailsWorker(ValueError("would be fatal"))
        proc = TaskProcessor(worker, policy)

        result = await proc.process("task")

        assert result is False
        assert worker.attempts == 3

    @pytest.mark.asyncio
    async def test_all_transient_classifier(self):
        classifier = AllTransient()
        assert classifier.classify(ValueError("x")) == ErrorKind.TRANSIENT
        assert classifier.classify(RuntimeError("x")) == ErrorKind.TRANSIENT
        assert classifier.classify(Exception("x")) == ErrorKind.TRANSIENT


class TestErrorClassificationInPollingLoop:
    @pytest.mark.asyncio
    async def test_fatal_error_stops_loop(self):
        calls: list[int] = []
        lifecycle = Lifecycle()

        async def callback() -> bool:
            calls.append(1)
            raise ValueError("fatal")

        loop = PollingLoop(
            callback, lifecycle, interval=0.01, jitter=0,
            error_classifier=FatalOnValueError(),
        )
        task = loop.start()
        await asyncio.sleep(0.05)

        assert len(calls) == 1
        assert task.done()

    @pytest.mark.asyncio
    async def test_transient_error_continues_loop(self):
        calls: list[int] = []
        lifecycle = Lifecycle()

        async def callback() -> bool:
            calls.append(1)
            raise RuntimeError("transient")

        loop = PollingLoop(
            callback, lifecycle, interval=0.01, jitter=0,
            error_classifier=FatalOnValueError(),
        )
        loop.start()
        await asyncio.sleep(0.05)
        lifecycle.close()

        assert len(calls) >= 3

    @pytest.mark.asyncio
    async def test_default_classifier_continues_on_all_errors(self):
        calls: list[int] = []
        lifecycle = Lifecycle()

        async def callback() -> bool:
            calls.append(1)
            raise ValueError("would be fatal with classifier")

        loop = PollingLoop(callback, lifecycle, interval=0.01, jitter=0)
        loop.start()
        await asyncio.sleep(0.05)
        lifecycle.close()

        assert len(calls) >= 3
