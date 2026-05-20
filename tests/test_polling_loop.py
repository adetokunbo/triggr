from __future__ import annotations

import asyncio
import pytest

from triggr import Lifecycle, PollingLoop


class TestPollingLoop:
    @pytest.mark.asyncio
    async def test_calls_callback_repeatedly(self):
        calls = []
        lifecycle = Lifecycle()

        async def callback() -> bool:
            calls.append(True)
            return False

        loop = PollingLoop(callback, lifecycle, interval=0.01, jitter=0)
        loop.start()
        await asyncio.sleep(0.05)
        lifecycle.close()

        assert len(calls) >= 3

    @pytest.mark.asyncio
    async def test_loops_immediately_on_true(self):
        calls = []
        lifecycle = Lifecycle()

        async def callback() -> bool:
            calls.append(True)
            if len(calls) >= 5:
                lifecycle.close()
            return True  # loop immediately

        loop = PollingLoop(callback, lifecycle, interval=1.0, jitter=0)
        task = loop.start()
        await task

        # Should have looped fast, not waited 1s between calls
        assert len(calls) >= 5

    @pytest.mark.asyncio
    async def test_pause_blocks_callback(self):
        calls = []
        lifecycle = Lifecycle()

        async def callback() -> bool:
            calls.append(True)
            return False

        loop = PollingLoop(callback, lifecycle, interval=0.01, jitter=0)
        loop.start()
        await asyncio.sleep(0.03)
        count_before = len(calls)

        lifecycle.pause()
        await loop.wait_for_work_finished()
        await asyncio.sleep(0.03)
        count_paused = len(calls)

        lifecycle.close()

        assert count_paused == count_before or count_paused == count_before + 1

    @pytest.mark.asyncio
    async def test_resume_restarts_callback(self):
        calls = []
        lifecycle = Lifecycle()

        async def callback() -> bool:
            calls.append(True)
            return False

        loop = PollingLoop(callback, lifecycle, interval=0.01, jitter=0)
        loop.start()
        await asyncio.sleep(0.02)

        lifecycle.pause()
        await loop.wait_for_work_finished()
        count_paused = len(calls)

        lifecycle.resume()
        await asyncio.sleep(0.03)
        count_resumed = len(calls)

        lifecycle.close()

        assert count_resumed > count_paused

    @pytest.mark.asyncio
    async def test_survives_callback_errors(self):
        calls = []
        lifecycle = Lifecycle()

        async def callback() -> bool:
            calls.append(True)
            if len(calls) <= 2:
                raise ValueError("oops")
            return False

        loop = PollingLoop(
            callback, lifecycle, interval=0.01, jitter=0, max_silent_failures=10
        )
        loop.start()
        await asyncio.sleep(0.05)
        lifecycle.close()

        assert len(calls) >= 4  # kept going after errors

    @pytest.mark.asyncio
    async def test_is_healthy(self):
        lifecycle = Lifecycle()

        async def callback() -> bool:
            return False

        loop = PollingLoop(callback, lifecycle, interval=0.01, jitter=0)
        assert not loop.is_healthy()

        loop.start()
        await asyncio.sleep(0.01)
        assert loop.is_healthy()

        lifecycle.close()
