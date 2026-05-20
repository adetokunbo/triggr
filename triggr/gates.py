from __future__ import annotations

import asyncio
from typing import Awaitable, Callable


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

    def __init__(self, *gates: object) -> None:
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
