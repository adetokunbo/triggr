from .config import AutomationConfig
from .lifecycle import Lifecycle
from .outcome import TaskOutcome
from .polling_loop import PollingLoop
from .processor import TaskProcessor
from .protocols import (
    AllTransient,
    ErrorClassifier,
    ErrorKind,
    HasHealth,
    NoOpMetrics,
    ReadinessGate,
    ReadyTaskLister,
    TaskSource,
    TaskWorker,
    TriggerMetrics,
)
from .gates import CompositeGate, EventGate, compose_gates
from .retry import AUTOMATION, LONG_RUNNING, RetryPolicy
from .scheduled import ReadyTask, ScheduledTaskSource
from .service import AutomationService
from .triggers import PeriodicTask, PeriodicTrigger, PollingTaskTrigger, StreamTaskTrigger

__all__ = [
    "AutomationConfig",
    "Lifecycle",
    "TaskOutcome",
    "PollingLoop",
    "TaskProcessor",
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
    "ReadyTaskLister",
    "TaskSource",
    "TaskWorker",
    "RetryPolicy",
    "AUTOMATION",
    "LONG_RUNNING",
    "ReadyTask",
    "ScheduledTaskSource",
    "PeriodicTask",
    "PeriodicTrigger",
    "PollingTaskTrigger",
    "AutomationService",
    "StreamTaskTrigger",
]
