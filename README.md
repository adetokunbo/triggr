# triggr

Composable async trigger framework for Python.

Provides building blocks for polling loops, parallel task execution,
stream-based processing, and long-running service management.

## Installation

```bash
pip install triggr
```

## Quick start

Implement `Source` and `Worker`, wire them into a trigger:

```python
from triggr import PollingTrigger, PollingConfig, Outcome

class PendingOrderSource:
    async def retrieve(self) -> list[Order]:
        return await db.fetch_pending_orders(limit=50)

class FulfillmentWorker:
    async def complete(self, order: Order) -> Outcome:
        if await warehouse.ship(order):
            return Outcome.SUCCESS
        return Outcome.FAILED

    async def is_stale(self, order: Order) -> bool:
        return await db.is_cancelled(order.id)

config = PollingConfig(polling_interval=30.0, parallelism=4)
trigger = PollingTrigger(PendingOrderSource(), FulfillmentWorker(), config)
trigger.run()
```

## Trigger types

| Type | Use when |
|------|----------|
| `PollingTrigger` | Tasks live in a store; poll on an interval |
| `StreamTrigger` | Tasks arrive via an `AsyncIterator` |
| `PeriodicTrigger` | Run a task on a fixed interval |
| `ScheduledSource` | Adapt a time-ready lister into a `PollingTrigger` source |

## Managing triggers together

`TriggerService` registers, starts, and monitors a named collection:

```python
from triggr import TriggerService, PollingTrigger, PeriodicTrigger, PollingConfig

config = PollingConfig(polling_interval=30.0, parallelism=4)

svc = TriggerService(expected={"orders", "inventory-sync"})
svc.register("orders", PollingTrigger(PendingOrderSource(), FulfillmentWorker(), config))
svc.register("inventory-sync", PeriodicTrigger(InventorySyncWorker(), interval=60.0))
svc.start_all()

svc.is_healthy()        # True if all triggers are healthy
svc.trigger_health()    # {"orders": True, "inventory-sync": True}
svc.close_all()
```

When `expected` is provided, `start_all()` raises if the registered set doesn't match exactly.

## Readiness gates

Gates block work until a condition is met. Pass one to any trigger via `ready_gate`:

```python
from triggr import EventGate, CompositeGate, PollingTrigger

db_gate = EventGate()
warehouse_gate = EventGate()
gate = CompositeGate(db_gate, warehouse_gate)

trigger = PollingTrigger(source, worker, config, ready_gate=gate)
trigger.run()

# From another coroutine, block until ready:
db_gate.set_not_ready()
await db.reconnect()
db_gate.set_ready()     # trigger resumes only when both gates are ready
```

## Long-running services

`RetryingService` keeps a `ManagedService` alive with two-level retry:

```python
from triggr import RetryingService, LONG_RUNNING

async def create_shipment_stream() -> ShipmentStreamService:
    client = await warehouse.connect()
    return ShipmentStreamService(client)

svc = RetryingService(factory=create_shipment_stream, retry_policy=LONG_RUNNING)
svc.run()
```

## Error classification

Implement `ErrorClassifier` to distinguish transient from fatal errors:

```python
from triggr import ErrorClassifier, ErrorKind

class PaymentGatewayClassifier:
    def classify(self, error: Exception) -> ErrorKind:
        if isinstance(error, GatewayTimeoutError):
            return ErrorKind.TRANSIENT
        return ErrorKind.FATAL
```

Pass it to any trigger or `RetryingService` via `error_classifier`.

## API reference

Full module documentation is in the module docstrings. Generated API docs coming soon.

## Development

Requires [Nix](https://nixos.org/) and [direnv](https://direnv.net/):

```bash
direnv allow   # activates the nix shell
pytest         # runs the full test suite
```
