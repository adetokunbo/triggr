from __future__ import annotations

import asyncio

import pytest

from triggr import (
    Lifecycle,
    Outcome,
    PollingLoop,
    Processor,
    RetryPolicy,
    StreamTrigger,
)

from .helpers import RecordingWorker


class RecordingMetrics:
    """Captures metric calls for assertions."""

    def __init__(self) -> None:
        self.iterations: list[float] = []
        self.outcomes: list[tuple[Outcome, float]] = []
        self.errors: list[Exception] = []

    def record_iteration(self, duration: float) -> None:
        self.iterations.append(duration)

    def record_outcome(self, outcome: Outcome, duration: float) -> None:
        self.outcomes.append((outcome, duration))

    def record_error(self, error: Exception) -> None:
        self.errors.append(error)


class TestProcessorMetrics:
    @pytest.mark.asyncio
    async def test_records_success_outcome(self):
        metrics = RecordingMetrics()
        worker = RecordingWorker[str]()
        policy = RetryPolicy(max_retries=1, initial_delay=0.001, max_delay=0.01)
        proc = Processor(worker, policy, metrics=metrics)

        await proc.process("task")

        assert len(metrics.outcomes) == 1
        assert metrics.outcomes[0][0] == Outcome.SUCCESS
        assert metrics.outcomes[0][1] > 0

    @pytest.mark.asyncio
    async def test_records_failed_outcome_on_retries_exhausted(self):
        metrics = RecordingMetrics()

        class AlwaysFails:
            async def complete(self, task: str) -> Outcome:
                raise ValueError("boom")

            async def is_stale(self, task: str) -> bool:
                return False

        policy = RetryPolicy(max_retries=1, initial_delay=0.001, max_delay=0.01)
        proc = Processor(AlwaysFails(), policy, metrics=metrics)

        await proc.process("task")

        assert len(metrics.outcomes) == 1
        assert metrics.outcomes[0][0] == Outcome.FAILED
        assert len(metrics.errors) >= 1

    @pytest.mark.asyncio
    async def test_records_stale_outcome(self):
        metrics = RecordingMetrics()
        worker = RecordingWorker[str](stale=True)
        worker.fail_next = ValueError("transient")
        policy = RetryPolicy(max_retries=1, initial_delay=0.001, max_delay=0.01)
        proc = Processor(worker, policy, metrics=metrics)

        await proc.process("task")

        assert len(metrics.outcomes) == 1
        assert metrics.outcomes[0][0] == Outcome.STALE

    @pytest.mark.asyncio
    async def test_records_error_on_failure(self):
        metrics = RecordingMetrics()
        worker = RecordingWorker[str](stale=False)
        worker.fail_next = ValueError("transient")
        policy = RetryPolicy(max_retries=1, initial_delay=0.001, max_delay=0.01)
        proc = Processor(worker, policy, metrics=metrics)

        await proc.process("task")

        assert len(metrics.errors) >= 1
        assert isinstance(metrics.errors[0], ValueError)


class TestPollingLoopMetrics:
    @pytest.mark.asyncio
    async def test_records_iteration_duration(self):
        metrics = RecordingMetrics()
        lifecycle = Lifecycle()

        async def callback() -> bool:
            return False

        loop = PollingLoop(callback, lifecycle, interval=0.01, jitter=0, metrics=metrics)
        loop.start()
        await asyncio.sleep(0.05)
        lifecycle.close()

        assert len(metrics.iterations) >= 2
        assert all(d >= 0 for d in metrics.iterations)

    @pytest.mark.asyncio
    async def test_no_iteration_recorded_on_error(self):
        metrics = RecordingMetrics()
        lifecycle = Lifecycle()
        calls = []

        async def callback() -> bool:
            calls.append(1)
            if len(calls) == 1:
                raise ValueError("fail")
            return False

        loop = PollingLoop(callback, lifecycle, interval=0.01, jitter=0, metrics=metrics)
        loop.start()
        await asyncio.sleep(0.05)
        lifecycle.close()

        assert len(metrics.iterations) == len(calls) - 1


class TestPollingLoopTimedHealth:
    @pytest.mark.asyncio
    async def test_healthy_after_completion(self):
        lifecycle = Lifecycle()

        async def callback() -> bool:
            return False

        loop = PollingLoop(callback, lifecycle, interval=0.01, jitter=0)
        loop.start()
        await asyncio.sleep(0.02)

        assert loop.is_healthy()
        lifecycle.close()

    @pytest.mark.asyncio
    async def test_unhealthy_after_grace_period(self):
        lifecycle = Lifecycle()
        call_count = 0

        async def callback() -> bool:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                await asyncio.sleep(10.0)  # stuck on second call
            return True  # loop immediately

        loop = PollingLoop(callback=callback, lifecycle=lifecycle, interval=0.01, jitter=0)
        loop._grace_period = 0.03

        loop.start()
        await asyncio.sleep(0.02)  # first completes, second starts and sticks
        await asyncio.sleep(0.05)  # past grace period

        assert not loop.is_healthy()
        lifecycle.close()


class TestStreamTriggerTimedHealth:
    @pytest.mark.asyncio
    async def test_unhealthy_after_grace_period(self):
        async def slow_stream():
            yield "first"
            await asyncio.sleep(10.0)  # stuck after first

        worker = RecordingWorker[str]()
        trigger = StreamTrigger(slow_stream(), worker, grace_period=0.03, name="test")

        trigger.run()
        await asyncio.sleep(0.02)  # first task completes
        assert trigger.is_healthy()

        await asyncio.sleep(0.05)  # past grace period
        assert not trigger.is_healthy()

        trigger.close()
