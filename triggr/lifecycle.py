"""Pause/resume/close state for a trigger and its components.

``Lifecycle`` is used internally by triggers. Each trigger owns one
instance and delegates its ``pause()``, ``resume()``, and ``close()``
methods to it, which is why those methods exist on every trigger type::

    trigger = PollingTrigger(source, worker, config)
    trigger.run()

    trigger.pause()    # delegates to lifecycle.pause() — blocks after current task
    trigger.resume()   # delegates to lifecycle.resume() — unblocks
    trigger.close()    # delegates to lifecycle.close() — shuts down permanently

To start a trigger suspended and resume it later — useful when registering
several triggers that should start together::

    svc = TriggerService()
    svc.register("orders", PollingTrigger(PendingOrderSource(), FulfillmentWorker(), config))
    svc.register("inventory-sync", PeriodicTrigger(InventorySyncWorker(), interval=60.0))
    svc.start_all(paused=True)

    await warm_up()

    svc.resume_all()

``Lifecycle`` can also be used directly when building a custom
``ManagedService`` that needs the same pause/resume semantics as triggers.
"""

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
