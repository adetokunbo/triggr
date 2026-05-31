from __future__ import annotations

import pytest

from triggr import Outcome, Processor, RetryPolicy

from .helpers import RecordingWorker


@pytest.fixture
def fast_retry():
    return RetryPolicy(max_retries=2, initial_delay=0.001, max_delay=0.01)


class TestProcessor:
    @pytest.mark.asyncio
    async def test_success(self, fast_retry):
        worker = RecordingWorker[str]()
        proc = Processor(worker, fast_retry)

        result = await proc.process("task1")

        assert result is True
        assert worker.completed == ["task1"]

    @pytest.mark.asyncio
    async def test_noop(self, fast_retry):
        worker = RecordingWorker[str](outcome=Outcome.NOOP)
        proc = Processor(worker, fast_retry)

        result = await proc.process("task1")

        assert result is False

    @pytest.mark.asyncio
    async def test_failed(self, fast_retry):
        worker = RecordingWorker[str](outcome=Outcome.FAILED)
        proc = Processor(worker, fast_retry)

        result = await proc.process("task1")

        assert result is False

    @pytest.mark.asyncio
    async def test_retries_then_succeeds(self, fast_retry):
        worker = RecordingWorker[str]()
        worker.fail_next = ValueError("transient")
        proc = Processor(worker, fast_retry)

        # First attempt fails, staleness check returns False, retry succeeds
        result = await proc.process("task1")

        assert result is True
        assert worker.completed == ["task1"]
        assert len(worker.stale_checks) == 1

    @pytest.mark.asyncio
    async def test_stale_on_failure(self, fast_retry):
        worker = RecordingWorker[str](stale=True)
        worker.fail_next = ValueError("transient")
        proc = Processor(worker, fast_retry)

        result = await proc.process("task1")

        assert result is True  # stale counts as success
        assert worker.completed == []  # never completed, was stale
        assert len(worker.stale_checks) == 1

    @pytest.mark.asyncio
    async def test_retries_exhausted(self):
        policy = RetryPolicy(max_retries=1, initial_delay=0.001, max_delay=0.001)

        class AlwaysFails:
            async def complete(self, task: str) -> Outcome:
                raise ValueError("permanent")

            async def is_stale(self, task: str) -> bool:
                return False

        proc = Processor(AlwaysFails(), policy)
        result = await proc.process("doomed")

        assert result is False

    @pytest.mark.asyncio
    async def test_ready_gate_called(self, fast_retry):
        gate_calls = []

        async def gate():
            gate_calls.append(True)

        worker = RecordingWorker[str]()
        proc = Processor(worker, fast_retry, ready_gate=gate)

        await proc.process("task1")

        assert len(gate_calls) == 1
