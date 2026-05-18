from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)


class Lifecycle:
    """Manages pause/resume/close for a component.

    Pause semantics: clearing the event causes waiters to block.
    Resume sets the event, unblocking all waiters.
    Close cancels any running task and marks the lifecycle as done.
    """

    def __init__(self, name: str = "") -> None:
        self._name = name
        self._not_paused = asyncio.Event()
        self._not_paused.set()
        self._closed = False

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def is_paused(self) -> bool:
        return not self._not_paused.is_set()

    def pause(self) -> None:
        self._not_paused.clear()

    def resume(self) -> None:
        self._not_paused.set()

    async def wait_for_not_paused(self) -> None:
        await self._not_paused.wait()

    def close(self) -> None:
        self._closed = True
        self._not_paused.set()  # unblock any waiters so they can exit
