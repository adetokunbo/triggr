from __future__ import annotations

import asyncio
import logging
import random
from abc import abstractmethod

from .base import Trigger, TriggerContext

logger = logging.getLogger(__name__)


class PollingTrigger(Trigger):
    """Trigger that runs a polling loop with configurable interval and jitter."""

    def __init__(self, context: TriggerContext, name: str | None = None) -> None:
        super().__init__(context, name)
        self._num_consecutive_failures = 0
        self._work_finished: asyncio.Event = asyncio.Event()
        self._work_finished.set()

    @abstractmethod
    async def perform_work_if_available(self) -> bool:
        """Do work if available. Return True to loop immediately, False to wait."""
        ...

    @property
    def _polling_interval(self) -> float:
        return self._context.config.polling_interval

    @property
    def _polling_jitter(self) -> float:
        return self._context.config.polling_jitter

    def _next_delay(self) -> float:
        interval = self._polling_interval
        jitter = interval * self._polling_jitter
        return interval + random.uniform(-jitter, jitter)

    async def _run(self) -> None:
        while not self._closed:
            try:
                await self.wait_for_not_paused()
                self._work_finished.clear()
                try:
                    await self.wait_for_ready()
                    has_more = await self.perform_work_if_available()
                    self._num_consecutive_failures = 0
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._num_consecutive_failures += 1
                    max_silent = self._context.config.max_num_silent_polling_retries
                    if self._num_consecutive_failures > max_silent:
                        self._logger.warning(
                            "Polling %s: %d consecutive failures: %s",
                            self._name,
                            self._num_consecutive_failures,
                            e,
                        )
                        self._num_consecutive_failures = 0
                    else:
                        self._logger.debug("Polling %s transient failure: %s", self._name, e)
                    has_more = False
                finally:
                    self._work_finished.set()

                if not has_more:
                    await asyncio.sleep(self._next_delay())

            except asyncio.CancelledError:
                break

    async def pause(self) -> None:
        self._paused.clear()
        await self._work_finished.wait()

    async def run_once(self) -> bool:
        """Run one iteration (for testing). Trigger must be paused."""
        assert not self._paused.is_set(), "Trigger must be paused to call run_once"
        return await self.perform_work_if_available()
