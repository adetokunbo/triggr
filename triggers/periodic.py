from __future__ import annotations

import time
from abc import abstractmethod
from dataclasses import dataclass

from .base import TriggerContext
from .polling import PollingTrigger
from .retry import AUTOMATION, RetryPolicy
from .task_based import TaskbasedTrigger, TaskOutcome

T_PERIODIC = "PeriodicTask"


@dataclass
class PeriodicTask:
    timestamp: float

    def __str__(self) -> str:
        return f"PeriodicTask({self.timestamp})"


class PeriodicTaskTrigger(PollingTrigger, TaskbasedTrigger[PeriodicTask]):
    """Run a task on a fixed interval. The task is never stale.

    Subclasses implement:
    - complete_task(task) -> TaskOutcome
    """

    def __init__(
        self,
        context: TriggerContext,
        execution_interval: float,
        name: str | None = None,
        retry_policy: RetryPolicy = AUTOMATION,
    ) -> None:
        PollingTrigger.__init__(self, context, name)
        TaskbasedTrigger.__init__(self, context, name, retry_policy)
        self._execution_interval = execution_interval

    @property
    def _polling_interval(self) -> float:
        return self._execution_interval

    async def perform_work_if_available(self) -> bool:
        task = PeriodicTask(timestamp=time.time())
        await self.process_task_with_retry(task)
        return False  # never loop immediately

    async def is_stale_task(self, task: PeriodicTask) -> bool:
        return False  # periodic tasks are never stale
