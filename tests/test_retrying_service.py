from __future__ import annotations

import asyncio
import pytest

from triggers import EventGate, Lifecycle, RetryingService, RetryPolicy


class FakeService:
    """A managed service that runs until closed or until it finishes on its own."""

    def __init__(self, run_duration: float | None = None, fail_after: float | None = None) -> None:
        self._run_duration = run_duration
        self._fail_after = fail_after
        self._task: asyncio.Task[None] | None = None
        self._closed = False
        self.started = False

    async def start(self) -> asyncio.Task[None]:
        self.started = True
        self._task = asyncio.ensure_future(self._run())
        return self._task

    async def _run(self) -> None:
        try:
            if self._fail_after is not None:
                await asyncio.sleep(self._fail_after)
                raise RuntimeError("service failed")
            elif self._run_duration is not None:
                await asyncio.sleep(self._run_duration)
            else:
                await asyncio.sleep(3600)  # run "forever"
        except asyncio.CancelledError:
            pass

    def close(self) -> None:
        self._closed = True
        if self._task and not self._task.done():
            self._task.cancel()

    def is_active(self) -> bool:
        return (
            self._task is not None
            and not self._task.done()
            and not self._closed
        )


class TestRetryingService:
    @pytest.mark.asyncio
    async def test_starts_service(self):
        service = FakeService()

        async def factory():
            return service

        rs = RetryingService(factory, restart_interval=0.01, name="test")
        rs.run()
        await asyncio.sleep(0.03)

        assert service.started
        assert service.is_active()
        rs.close()

    @pytest.mark.asyncio
    async def test_restarts_after_unexpected_completion(self):
        call_count = 0

        async def factory():
            nonlocal call_count
            call_count += 1
            return FakeService(run_duration=0.02)

        rs = RetryingService(factory, restart_interval=0.01, name="test")
        rs.run()
        await asyncio.sleep(0.1)
        rs.close()

        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_retries_factory_on_failure(self):
        call_count = 0

        async def factory():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise ValueError(f"attempt {call_count}")
            return FakeService()

        policy = RetryPolicy(max_retries=5, initial_delay=0.001, max_delay=0.01)
        rs = RetryingService(factory, retry_policy=policy, restart_interval=0.01, name="test")
        rs.run()
        await asyncio.sleep(0.05)

        assert call_count >= 3
        assert rs.is_healthy()
        rs.close()

    @pytest.mark.asyncio
    async def test_outer_restart_after_inner_retries_exhausted(self):
        call_count = 0

        async def factory():
            nonlocal call_count
            call_count += 1
            raise ValueError("always fails")

        policy = RetryPolicy(max_retries=1, initial_delay=0.001, max_delay=0.001)
        rs = RetryingService(factory, retry_policy=policy, restart_interval=0.02, name="test")
        rs.run()
        await asyncio.sleep(0.1)
        rs.close()

        # Inner retries: 2 per attempt (1 + 1 retry). Outer restarts multiple times.
        assert call_count >= 4

    @pytest.mark.asyncio
    async def test_pause_blocks_restart(self):
        call_count = 0

        async def factory():
            nonlocal call_count
            call_count += 1
            return FakeService(run_duration=0.01)

        rs = RetryingService(factory, restart_interval=0.01, name="test")
        rs.run()
        await asyncio.sleep(0.03)
        count_before = call_count

        rs.pause()
        await asyncio.sleep(0.05)
        count_paused = call_count

        rs.close()

        assert count_paused <= count_before + 1

    @pytest.mark.asyncio
    async def test_resume_after_pause(self):
        call_count = 0

        async def factory():
            nonlocal call_count
            call_count += 1
            return FakeService(run_duration=0.01)

        rs = RetryingService(factory, restart_interval=0.01, name="test")
        rs.run()
        await asyncio.sleep(0.03)

        rs.pause()
        await asyncio.sleep(0.03)
        count_paused = call_count

        rs.resume()
        await asyncio.sleep(0.05)

        rs.close()
        assert call_count > count_paused

    @pytest.mark.asyncio
    async def test_close_shuts_down_inner_service(self):
        service = FakeService()

        async def factory():
            return service

        rs = RetryingService(factory, restart_interval=0.01, name="test")
        rs.run()
        await asyncio.sleep(0.03)
        assert service.is_active()

        rs.close()
        await asyncio.sleep(0.01)

        assert not service.is_active()

    @pytest.mark.asyncio
    async def test_is_healthy(self):
        service = FakeService()

        async def factory():
            return service

        rs = RetryingService(factory, restart_interval=0.01, name="test")
        rs.run()
        await asyncio.sleep(0.03)

        assert rs.is_healthy()

        rs.close()
        await asyncio.sleep(0.01)
        assert not rs.is_healthy()

    @pytest.mark.asyncio
    async def test_blocks_on_ready_gate(self):
        gate = EventGate()
        gate.set_not_ready()
        service = FakeService()

        async def factory():
            return service

        rs = RetryingService(
            factory, restart_interval=0.01, ready_gate=gate, name="test"
        )
        rs.run()
        await asyncio.sleep(0.03)

        assert not service.started

        gate.set_ready()
        await asyncio.sleep(0.03)

        assert service.started
        rs.close()
