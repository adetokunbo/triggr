"""Source adapter for time-based task retrieval.

``ScheduledSource`` wraps a ``ReadyLister[T]`` and exposes it as a
``Source[ReadyTask[T]]``. It queries for items whose scheduled time has
passed and wraps each one in a ``ReadyTask`` before handing it to the
worker — so the worker receives a ``ReadyTask[Shipment]``, not a raw
``Shipment``.

End-to-end: implement ``ReadyLister``, wrap it in ``ScheduledSource``,
and implement ``Worker[ReadyTask[T]]``::

    from triggr import ScheduledSource, PollingTrigger, TriggerConfig, ReadyTask, Outcome
    import time

    class ScheduledShipmentLister:
        async def list_ready(self, now: float, limit: int) -> list[Shipment]:
            return await db.fetch_shipments(dispatch_before=now, limit=limit)

    class FulfillmentWorker:
        async def complete(self, task: ReadyTask[Shipment]) -> Outcome:
            age = time.time() - task.ready_at
            if age > MAX_DELAY:
                await notify_delay(task.work)
            await warehouse.ship(task.work)
            return Outcome.SUCCESS

        async def is_stale(self, task: ReadyTask[Shipment]) -> bool:
            return await db.is_cancelled(task.work.id)

    source = ScheduledSource(lister=ScheduledShipmentLister(), parallelism=4)
    trigger = PollingTrigger(source, FulfillmentWorker(), TriggerConfig())

``ready_at`` is the Unix timestamp recorded when the task was retrieved.
The worker can use it to measure how long a shipment has been waiting.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Generic, TypeVar

from .protocols import ReadyLister

T = TypeVar("T")


@dataclass
class ReadyTask(Generic[T]):
    ready_at: float
    work: T

    def __str__(self) -> str:
        return f"ReadyTask(ready_at={self.ready_at}, work={self.work})"


class ScheduledSource(Generic[T]):
    """Source adapter that wraps a ReadyLister.

    Queries for tasks that are ready at the current time, wraps them
    in ReadyTask. Plugs into PollingTrigger as a Source.
    """

    def __init__(self, lister: ReadyLister[T], parallelism: int) -> None:
        self._lister = lister
        self._parallelism = parallelism

    async def retrieve(self) -> list[ReadyTask[T]]:
        now = time.time()
        items = await self._lister.list_ready(now, self._parallelism)
        return [ReadyTask(ready_at=now, work=item) for item in items]
