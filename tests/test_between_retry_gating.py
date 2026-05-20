from __future__ import annotations

import asyncio
import pytest

from triggr import Lifecycle, Processor, Outcome, RetryPolicy


class FailThenSucceedWorker:
    """Fails N times, then succeeds."""

    def __init__(self, fail_count: int) -> None:
        self._fail_count = fail_count
        self._attempts = 0

    async def complete(self, task: str) -> Outcome:
        self._attempts += 1
        if self._attempts <= self._fail_count:
            raise ValueError(f"attempt {self._attempts}")
        return Outcome.SUCCESS

    async def is_stale(self, task: str) -> bool:
        return False

    @property
    def attempts(self) -> int:
        return self._attempts


class TestBetweenRetryGating:
    @pytest.mark.asyncio
    async def test_gate_called_between_retries(self):
        gate_calls: list[bool] = []

        async def gate():
            gate_calls.append(True)

        policy = RetryPolicy(max_retries=3, initial_delay=0.001, max_delay=0.01)
        worker = FailThenSucceedWorker(fail_count=2)
        proc = Processor(worker, policy, ready_gate=gate)

        result = await proc.process("task")

        assert result is True
        assert worker.attempts == 3
        # Gate called: once before each attempt (3) + once between each retry (2) = 5
        assert len(gate_calls) == 5

    @pytest.mark.asyncio
    async def test_pause_delays_retry(self):
        lifecycle = Lifecycle()
        policy = RetryPolicy(max_retries=5, initial_delay=0.001, max_delay=0.01)
        worker = FailThenSucceedWorker(fail_count=1)
        proc = Processor(worker, policy, ready_gate=lifecycle.wait_for_not_paused)

        async def pause_then_resume():
            await asyncio.sleep(0.01)
            lifecycle.pause()
            await asyncio.sleep(0.05)
            lifecycle.resume()

        asyncio.ensure_future(pause_then_resume())
        result = await proc.process("task")

        assert result is True
        assert worker.attempts == 2

    @pytest.mark.asyncio
    async def test_no_gate_still_works(self):
        policy = RetryPolicy(max_retries=3, initial_delay=0.001, max_delay=0.01)
        worker = FailThenSucceedWorker(fail_count=1)
        proc = Processor(worker, policy, ready_gate=None)

        result = await proc.process("task")

        assert result is True
        assert worker.attempts == 2
