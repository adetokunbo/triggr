"""Readiness gates: conditions that must be met before a trigger proceeds.

A gate is any object with a ``wait_until_ready`` coroutine. Pass one to a
trigger via ``ready_gate``; the trigger blocks on it before each attempt::

    gate = EventGate()            # starts ready by default

    trigger = PollingTrigger(source, worker, config, ready_gate=gate)
    trigger.run()

    # From another coroutine, block the trigger until the warehouse is ready:
    gate.set_not_ready()
    await warehouse.wait_for_connection()
    gate.set_ready()

To require multiple conditions simultaneously, use ``CompositeGate``::

    db_gate = EventGate()
    warehouse_gate = EventGate()
    gate = CompositeGate(db_gate, warehouse_gate)

    trigger = PollingTrigger(source, worker, config, ready_gate=gate)
    trigger.run()

    db_gate.set_not_ready()       # trigger pauses; warehouse_gate still ready
    db_gate.set_ready()           # trigger resumes only when both are ready

For plain async callables (no full ``ReadinessGate`` object needed),
``compose_gates`` combines them without requiring the protocol::

    gate = compose_gates(wait_for_db, wait_for_warehouse)
    # gate() awaits both concurrently via asyncio.gather
"""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

from .protocols import ReadinessGate


class EventGate:
    """Readiness gate backed by an asyncio.Event.

    Ready by default. Call set_not_ready() to block, set_ready() to unblock.
    """

    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._event.set()

    def set_ready(self) -> None:
        self._event.set()

    def set_not_ready(self) -> None:
        self._event.clear()

    async def wait_until_ready(self) -> None:
        await self._event.wait()


class CompositeGate:
    """AND-combines multiple readiness gates.

    All gates must be ready before the composite is ready.
    """

    def __init__(self, *gates: ReadinessGate) -> None:
        self._gates = gates

    async def wait_until_ready(self) -> None:
        await asyncio.gather(*(g.wait_until_ready() for g in self._gates))


def compose_gates(
    *gates: Callable[[], Awaitable[None]],
) -> Callable[[], Awaitable[None]]:
    """Combine multiple gate callables into one that waits for all."""

    async def combined() -> None:
        await asyncio.gather(*(g() for g in gates))

    return combined
