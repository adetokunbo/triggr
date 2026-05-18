from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class AutomationConfig:
    polling_interval: float = 30.0
    polling_jitter: float = 0.2
    parallelism: int = 4
    max_num_silent_polling_retries: int = 3


@dataclass
class TriggerContext:
    config: AutomationConfig = field(default_factory=AutomationConfig)
    enabled: asyncio.Event = field(default_factory=asyncio.Event)

    def __post_init__(self) -> None:
        self.enabled.set()

    async def wait_for_ready(self) -> None:
        await self.enabled.wait()


class Trigger:
    """Base class for all triggers.

    Provides pause/resume lifecycle and readiness gating.
    """

    def __init__(self, context: TriggerContext, name: str | None = None) -> None:
        self._context = context
        self._name = name or type(self).__name__
        self._paused = asyncio.Event()
        self._paused.set()  # starts unpaused
        self._running_task: asyncio.Task[None] | None = None
        self._closed = False
        self._logger = logging.getLogger(f"triggers.{self._name}")

    @property
    def context(self) -> TriggerContext:
        return self._context

    def run(self, paused: bool = False) -> None:
        if paused:
            self._paused.clear()
        self._running_task = asyncio.ensure_future(self._run())

    async def _run(self) -> None:
        raise NotImplementedError

    async def pause(self) -> None:
        self._paused.clear()

    def resume(self) -> None:
        self._paused.set()

    async def wait_for_not_paused(self) -> None:
        await self._paused.wait()

    async def wait_for_ready(self) -> None:
        await self.wait_for_not_paused()
        await self._context.wait_for_ready()

    def is_healthy(self) -> bool:
        if self._running_task is None:
            return False
        return not self._running_task.done()

    async def close(self) -> None:
        self._closed = True
        if self._running_task and not self._running_task.done():
            self._running_task.cancel()
            try:
                await self._running_task
            except asyncio.CancelledError:
                pass
