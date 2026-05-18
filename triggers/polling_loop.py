from __future__ import annotations

import asyncio
import logging
import random
from typing import Awaitable, Callable

from .lifecycle import Lifecycle
from .protocols import AllTransient, ErrorClassifier, ErrorKind

logger = logging.getLogger(__name__)


class PollingLoop:
    """Standalone polling loop that calls a callback on an interval.

    Not tied to triggers — can drive any periodic async operation.
    The callback returns True to loop immediately (more work available),
    False to wait for the next interval.
    """

    def __init__(
        self,
        callback: Callable[[], Awaitable[bool]],
        lifecycle: Lifecycle,
        interval: float,
        jitter: float = 0.2,
        max_silent_failures: int = 3,
        error_classifier: ErrorClassifier | None = None,
        name: str = "",
    ) -> None:
        self._callback = callback
        self._lifecycle = lifecycle
        self._interval = interval
        self._jitter = jitter
        self._max_silent_failures = max_silent_failures
        self._classifier = error_classifier or AllTransient()
        self._name = name
        self._consecutive_failures = 0
        self._work_finished = asyncio.Event()
        self._work_finished.set()
        self._task: asyncio.Task[None] | None = None
        self._logger = logging.getLogger(f"polling.{name}" if name else __name__)

    def start(self) -> asyncio.Task[None]:
        self._task = asyncio.ensure_future(self._run())
        return self._task

    def is_healthy(self) -> bool:
        return self._task is not None and not self._task.done()

    async def wait_for_work_finished(self) -> None:
        await self._work_finished.wait()

    async def _run(self) -> None:
        while not self._lifecycle.is_closed:
            try:
                await self._lifecycle.wait_for_not_paused()
                if self._lifecycle.is_closed:
                    break

                self._work_finished.clear()
                try:
                    has_more = await self._callback()
                    self._consecutive_failures = 0
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    kind = self._classifier.classify(e)
                    if kind == ErrorKind.FATAL:
                        self._logger.error("Fatal error in polling loop: %s", e)
                        break
                    self._consecutive_failures += 1
                    if self._consecutive_failures > self._max_silent_failures:
                        self._logger.warning(
                            "%d consecutive failures: %s",
                            self._consecutive_failures,
                            e,
                        )
                        self._consecutive_failures = 0
                    else:
                        self._logger.debug("Transient failure: %s", e)
                    has_more = False
                finally:
                    self._work_finished.set()

                if not has_more:
                    delay = self._next_delay()
                    await asyncio.sleep(delay)

            except asyncio.CancelledError:
                break

    def _next_delay(self) -> float:
        jitter_range = self._interval * self._jitter
        return self._interval + random.uniform(-jitter_range, jitter_range)
