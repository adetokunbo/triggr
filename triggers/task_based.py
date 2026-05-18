from __future__ import annotations

import logging
from abc import abstractmethod
from enum import Enum, auto
from typing import Generic, TypeVar

from .base import Trigger, TriggerContext
from .retry import AUTOMATION, RetryPolicy, RetriesExhausted, retry

T = TypeVar("T")

logger = logging.getLogger(__name__)


class TaskOutcome(Enum):
    SUCCESS = auto()
    FAILED = auto()
    NOOP = auto()
    STALE = auto()


class TaskbasedTrigger(Trigger, Generic[T]):
    """Trigger that processes typed tasks with retry and staleness detection."""

    def __init__(
        self,
        context: TriggerContext,
        name: str | None = None,
        retry_policy: RetryPolicy = AUTOMATION,
    ) -> None:
        super().__init__(context, name)
        self._retry_policy = retry_policy

    @abstractmethod
    async def complete_task(self, task: T) -> TaskOutcome:
        """Execute the task. Must make progress so is_stale_task returns True after."""
        ...

    @abstractmethod
    async def is_stale_task(self, task: T) -> bool:
        """Check if the task can be skipped (already completed or superseded)."""
        ...

    async def process_task_with_retry(self, task: T) -> bool:
        """Process a single task with retry. Returns True if task succeeded or was stale."""
        try:
            outcome = await retry(
                self._retry_policy,
                lambda: self._attempt_task(task),
                description=f"{self._name}:{task}",
            )
        except RetriesExhausted as e:
            self._logger.error("Task %s failed after retries: %s", task, e.last_error)
            return False

        if outcome == TaskOutcome.SUCCESS:
            self._logger.info("Task %s completed successfully", task)
            return True
        elif outcome == TaskOutcome.STALE:
            self._logger.debug("Task %s is stale, skipping", task)
            return True
        elif outcome == TaskOutcome.FAILED:
            self._logger.warning("Task %s failed", task)
            return False
        else:  # NOOP
            return False

    async def _attempt_task(self, task: T) -> TaskOutcome:
        await self.wait_for_ready()
        try:
            return await self.complete_task(task)
        except Exception:
            if await self.is_stale_task(task):
                return TaskOutcome.STALE
            raise
