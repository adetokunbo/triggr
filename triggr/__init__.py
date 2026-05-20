"""triggr — composable async trigger framework.

Provides building blocks for polling loops, task processing, stream-based
triggers, and long-running service management. All built on asyncio with
no external runtime dependencies.

Typical usage::

    from triggr import PollingTrigger, AutomationConfig, Outcome

    class MySource:
        async def retrieve_tasks(self) -> list[str]:
            return ["work-item"]

    class MyWorker:
        async def complete(self, task: str) -> Outcome:
            print(f"processing {task}")
            return Outcome.SUCCESS

        async def is_stale(self, task: str) -> bool:
            return False

    config = AutomationConfig(polling_interval=5.0)
    trigger = PollingTrigger(MySource(), MyWorker(), config)
    trigger.run()
"""

from .config import AutomationConfig
from .lifecycle import Lifecycle
from .outcome import Outcome
from .polling_loop import PollingLoop
from .processor import Processor
from .protocols import (
    AllTransient,
    ErrorClassifier,
    ErrorKind,
    HasHealth,
    ManagedService,
    NoOpMetrics,
    ReadinessGate,
    ReadyLister,
    Source,
    Worker,
    TriggerMetrics,
)
from .gates import CompositeGate, EventGate, compose_gates
from .retry import AUTOMATION, LONG_RUNNING, RetryPolicy
from .scheduled import ReadyTask, ScheduledSource
from .retrying_service import RetryingService
from .service import AutomationService
from .triggers import PeriodicTask, PeriodicTrigger, PollingTrigger, StreamTrigger

__all__ = [
    "AutomationConfig",
    "Lifecycle",
    "Outcome",
    "PollingLoop",
    "Processor",
    "AllTransient",
    "ErrorClassifier",
    "ErrorKind",
    "CompositeGate",
    "EventGate",
    "compose_gates",
    "HasHealth",
    "ReadinessGate",
    "NoOpMetrics",
    "TriggerMetrics",
    "ReadyLister",
    "Source",
    "Worker",
    "RetryPolicy",
    "AUTOMATION",
    "LONG_RUNNING",
    "ReadyTask",
    "ScheduledSource",
    "PeriodicTask",
    "PeriodicTrigger",
    "PollingTrigger",
    "ManagedService",
    "RetryingService",
    "AutomationService",
    "StreamTrigger",
]
