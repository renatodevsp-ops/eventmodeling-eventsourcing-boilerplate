---
name: processor
description: Knows how to build processor / automation slices (Go, terraskye/eventsourcing)
---
# PROCESSOR Slice Skill

## Overview

A PROCESSOR slice (called an **automation** in event-modeling notation) reacts to a domain
event by issuing a **command**. It closes the loop: `event → command → event`. It produces no
read model and owns no business rules.

| Slice type | Reacts to | Produces | Touches read models |
|------------|-----------|----------|---------------------|
| STATE_CHANGE | a command | events | no |
| STATE_VIEW | events | read model | **yes** |
| **PROCESSOR** | **events** | **a command** | no |

The processor's only job is **translation**: turn the fields of an incoming event into the
fields of a command. Everything else — validation, idempotency, business rules — lives in the
command handler's `decide`. If a processor starts growing an `if` about domain state, that
logic belongs in the command.

## Flow Pattern

```
Event → EventBus.Subscribe → EventGroupProcessor → On<Event> → CommandHandler[C] → EventStore
```

```
POST /wallets/{id}/close-month → CloseMonthWallet → WalletMonthClosed
                                                              │
                              EventBus.Subscribe("freeze-wallet-processor")
                                                              ▼
                                     Processor.OnWalletMonthClosed
                                                              │  go func(){ <-time.After(delay) }
                                                              ▼
                                     FreezeWallet → WalletFrozen
```

Note the processor calls the `CommandHandler` **directly** (it is injected in its constructor).
There is no HTTP and no CommandBus hop — the processor is the actor.

---

## Anatomy

**Location:** `<module>/slices/<commandname>/processor.go` — the processor lives in the same
package as the command it issues, because it depends on that command's Go type.

| Symbol | Purpose |
|--------|---------|
| `Processor` | Struct holding the injected `CommandHandler[C]` + any config (delay, interval) |
| `NewProcessor(handler, ...)` | Constructor; takes the command handler as a dependency |
| `(*Processor).On<Event>` | Method per trigger event, receives `ctx context.Context, e *events.<Event>` |
| `(*Processor).EventHandlers()` | Returns `*eventsourcing.EventGroupProcessor` to pass to `eventBus.Subscribe` |
| `eventsourcing.OnEvent(p.On<Event>)` | Routes one concrete event type to one method |
| `eventsourcing.CommandHandler[C]` | `func(ctx, C) (eventsourcing.AppendResult, error)` — a func type, so tests stub it in one line |
| `eventsourcing.AppendResult` | Return of the command handler: `Successful`, `StreamID`, `NextExpectedVersion` |

Rules the library enforces for you:

- `OnEvent(fn)` requires a **pointer** arg (`*events.WalletMonthClosed`). Events are rehydrated
  as pointers; a value-typed handler compiles, is registered, and is then **never called**
  (`Handle` type-asserts to the pointer and returns `SkippedEventError`).
- `NewEventGroupProcessor` **panics** on duplicate `EventName()` — never register the same
  event type twice.
- `EventGroupProcessor.StreamFilter()` returns the registered event type names — feed it to
  `postgresbus.WithFilterEvents(...)` so the subscriber does not receive the whole stream.

---

## Template: event-driven processor

```go
package <commandname>

import (
    "context"
    "log"
    "time"

    "github.com/renatodevsp-ops/eventmodeling-eventsourcing-boilerplate/<module>/events"
    "github.com/terraskye/eventsourcing"
)

type Processor struct {
    handler eventsourcing.CommandHandler[<CommandName>]
    delay   time.Duration
}

func NewProcessor(handler eventsourcing.CommandHandler[<CommandName>], delay time.Duration) *Processor {
    return &Processor{handler: handler, delay: delay}
}

func (p *Processor) On<TriggerEvent>(ctx context.Context, e *events.<TriggerEvent>) error {
    go func() {
        select {
        case <-time.After(p.delay):
            cmd := <CommandName>{<IDFieldName>: e.<IDFieldName>}
            if _, err := p.handler(context.Background(), cmd); err != nil {
                log.Printf("<do something> %s: %v", e.<IDFieldName>, err)
            }
        case <-ctx.Done():
            // Subscription cancelled; skip
        }
    }()

    return nil
}

func (p *Processor) EventHandlers() *eventsourcing.EventGroupProcessor {
    return eventsourcing.NewEventGroupProcessor(
        eventsourcing.OnEvent(p.On<TriggerEvent>),
    )
}
```

### Rules of the pattern

1. **Return `nil` from the handler.** Delivery is asynchronous; a returned error is only logged
   by the bus and does not retry. Handle the failure where you log it.
2. **Never use the event `ctx` after returning.** `Handle` returns before the goroutine runs.
   Pass `context.Background()` to the command handler and keep `ctx` only for the `ctx.Done()`
   select.
3. **`delay` is a knob, not a constant.** Pass `5*time.Second` in `main.go` for dev, real
   durations in production — never hardcode `30*24*time.Hour` in the slice.
4. **A `goroutine` is not durable.** It dies with the process. If the work must survive a
   restart, use Variant B (polling a TODO list) instead.
5. **The command must be idempotent** — see below.

### Idempotency (non-negotiable)

The bus delivers at-least-once, and a retry fires the command again. The **command handler**
absorbs that, by returning no events when the work is already done:

```go
func decide(state <state>, cmd <CommandName>) ([]eventsourcing.Event, error) {
    if !state.<precondition> {
        return nil, fmt.Errorf("<aggregate> %s is not <precondition>", cmd.<IDFieldName>)
    }
    if state.<alreadyDone> {
        return nil, nil // idempotent — no new events, no error
    }
    return []eventsourcing.Event{&events.<EventName>{...}}, nil
}
```

`decide` returning `(nil, nil)` makes `NewCommandHandler` skip `EventStore.Save` and return a
successful `AppendResult`. A processor that re-issues the command therefore becomes a no-op.

---

## Variant A: event-driven (`trigger`)

The default. Use it when **one event is enough to decide** and the work is quick and safe to
repeat (see the template above).

Subscribe with a filter so the subscriber does not receive the whole stream:

```go
handlers := processor.EventHandlers()
traced := otel.WithEventTelemetry(handlers, otel.WithOperation("<SliceName>Processor"))

if err := eventBus.Subscribe(ctx, "<commandname>-processor", traced,
    postgresbus.WithFilterEvents(handlers.StreamFilter()),
); err != nil {
    log.Fatal(err)
}
```

`WithFilterEvents` lives in `github.com/terraskye/eventsourcing/eventbus/postgres` (and panics
if handed to a bus from another package). `postgresbus.WithStartFrom(pos)` sets the global
event position a **new** subscription starts from — an established subscription resumes from
its stored position regardless.

---

## Variant B: polling a TODO list (`polls`)

Use it when the work is **slow, calls another system, may fail, or must not run twice**. A
projection is the queue: it gains a row when work becomes due and marks it done when the
finishing event arrives. The processor then reads the open rows on its own schedule.

```
Event → State View (TODO list read model) ──ticker──> Processor → Command → Event → row marked done
```

The library ships **no scheduler** — the loop is a `time.Ticker` (or a cron) in `main.go`. The
processor side is a plain query plus a loop over open rows:

```go
// Sketch — this repo queries Postgres directly, there is no generic Repository[T] here.
type Processor struct {
    handler eventsourcing.CommandHandler[<CommandName>]
    pending func(ctx context.Context) ([]<TodoRow>, error) // read model: open rows only
    every   time.Duration
}

func (p *Processor) Run(ctx context.Context) {
    t := time.NewTicker(p.every)
    defer t.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-t.C:
            rows, err := p.pending(ctx)
            if err != nil {
                log.Printf("read todo list: %v", err)
                continue
            }
            for _, row := range rows {
                cmd := <CommandName>{<IDFieldName>: row.ID}
                if _, err := p.handler(ctx, cmd); err != nil {
                    log.Printf("<do something> %s: %v", row.ID, err)
                }
            }
        }
    }
}
```

What the TODO list buys over Variant A:

| | Variant A (`trigger`) | Variant B (`polls`) |
|---|---|---|
| Survives restart | no | yes — the list is built from events |
| Runs twice | possible | no — a done row is not reprocessed |
| State to reason about | none | pending / done / stuck |

---

## Variant C: translation of an external event

An event from **outside** the system boundary (webhook, Kafka topic, message queue) arrives on
the same bus and is translated into an internal command. Same `On<ExternalEvent>` shape as
Variant A; what differs is the data source and the responsibilities:

- Map the payload to command fields explicitly — the external payload is not your domain model.
- Carry the external identifier and use it as the idempotency key, so a retried delivery lands
  on the same aggregate stream instead of creating a duplicate.
- Never trust the incoming `AggregateID()`; map it to a local ID.

```go
func (p *Processor) On<ExternalEvent>(ctx context.Context, e *events.<ExternalEvent>) error {
    cmd := <CommandName>{
        <IDFieldName>: e.<ExternalRefID>, // local ID carried by the external payload
    }
    if _, err := p.handler(context.Background(), cmd); err != nil {
        log.Printf("<do something> %s: %v", e.<ExternalRefID>, err)
    }
    return nil
}
```

---

## Specifications (Test Cases)

Test the processor with a **stub command handler**. `eventsourcing.CommandHandler[C]` is a
plain `func` type, so the stub is one line and no event store is needed — the assertion is
"this event produced this command".

Use:
- Standard `testing` package — no testify
- Table-driven tests — one `tests` slice, one `t.Run` loop
- `delay: 0` plus a channel, since the handler dispatches in a goroutine
- **pass a pointer event** (`&events.X{}`) to `Handle`, or the handler is skipped

```go
package <commandname>

import (
    "context"
    "testing"
    "time"

    "github.com/google/uuid"
    "github.com/renatodevsp-ops/eventmodeling-eventsourcing-boilerplate/<module>/events"
    "github.com/terraskye/eventsourcing"
)

func TestProcessor_On<TriggerEvent>(t *testing.T) {
    tests := []struct {
        name  string
        event eventsourcing.Event
        want  <CommandName>
    }{
        {
            name: "spec: <SliceName> - issues <CommandName> for the closed aggregate",
            event: &events.<TriggerEvent>{
                <IDFieldName>: uuid.MustParse("00000000-0000-0000-0000-000000000001"),
            },
            want: <CommandName>{<IDFieldName>: uuid.MustParse("00000000-0000-0000-0000-000000000001")},
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            issued := make(chan <CommandName>, 1)
            handler := func(ctx context.Context, cmd <CommandName>) (eventsourcing.AppendResult, error) {
                issued <- cmd
                return eventsourcing.AppendResult{Successful: true}, nil
            }

            p := NewProcessor(handler, 0)
            if err := p.EventHandlers().Handle(context.Background(), tt.event); err != nil {
                t.Fatalf("Handle: %v", err)
            }

            select {
            case got := <-issued:
                if got != tt.want {
                    t.Errorf("issued command = %+v, want %+v", got, tt.want)
                }
            case <-time.After(time.Second):
                t.Fatal("no command issued")
            }
        })
    }
}
```

Use the same table-driven style for `decide` in `command.go` (see the `state-change` skill) —
the processor spec proves the translation, the command spec proves the rule.

---

## Wiring in `main.go`

All registrations are centralized in `main.go`. Do **not** touch the existing handler wiring.

```go
freezeWalletHandler := walletfreezing.NewHandler(tracedEventStore)
tracedFreezeWalletHandler := otel.WithCommandTelemetry(freezeWalletHandler, otel.WithOperation("FreezeWallet"))

freezeWalletProcessor := walletfreezing.NewProcessor(tracedFreezeWalletHandler, 5*time.Second)
tracedProcessorHandlers := otel.WithEventTelemetry(freezeWalletProcessor.EventHandlers(), otel.WithOperation("WalletFreezingProcessor"))

if err := eventBus.Subscribe(ctx, "freeze-wallet-processor", tracedProcessorHandlers); err != nil {
    log.Fatal(err)
}
```

Order matters: wrap the command handler first, build the processor from the wrapped handler,
then wrap the processor's `EventHandlers()` for tracing, then subscribe under a unique name
(`<commandname>-processor`) — a duplicate name makes `Subscribe` fail.

---

## Naming

| Element | Convention | Example |
|---------|------------|---------|
| Subscription name | `<commandname>-processor` | `freeze-wallet-processor` |
| Telemetry operation | `<SliceName>Processor` | `WalletFreezingProcessor` |
| Handler method | `On<TriggerEvent>` | `OnWalletMonthClosed` |

When documenting the slice in the event model, name the **automation** after what it does, not
after what triggers it — the trigger is already visible. `OnWalletMonthClosed` is right for the
Go method; the model card should read `WalletFreezer`, not `OnWalletMonthClosed`.

---

## Package Structure

```
<module>/
├── events/
│   └── <eventname>.go            # trigger events come from here (owned by other slices)
│
└── slices/
    └── <commandname>/
        ├── command.go            # Command, state, evolve, decide, NewHandler
        ├── processor.go          # Processor, NewProcessor, On<Event>, EventHandlers
        └── processor_test.go     # spec: event → command
```

---

## Complete Example: "Freeze Wallet After Month Close"

**Slice definition:**

| Element | Value |
|---------|-------|
| Trigger | `WalletMonthClosed(walletId, month, year, closedBy, closedAt)` |
| Command | `FreezeWallet(walletId)` |
| Event | `WalletFrozen(walletId, archivedAt)` |

**`wallet-management/slices/walletfreezing/processor.go`:**
```go
package walletfreezing

import (
    "context"
    "log"
    "time"

    "github.com/renatodevsp-ops/eventmodeling-eventsourcing-boilerplate/wallet-management/events"
    "github.com/terraskye/eventsourcing"
)

type Processor struct {
    handler eventsourcing.CommandHandler[FreezeWallet]
    delay   time.Duration
}

func NewProcessor(handler eventsourcing.CommandHandler[FreezeWallet], delay time.Duration) *Processor {
    return &Processor{handler: handler, delay: delay}
}

func (p *Processor) OnWalletMonthClosed(ctx context.Context, e *events.WalletMonthClosed) error {
    go func() {
        select {
        case <-time.After(p.delay):
            cmd := FreezeWallet{WalletID: e.WalletID}
            if _, err := p.handler(context.Background(), cmd); err != nil {
                log.Printf("freeze wallet %s: %v", e.WalletID, err)
            }
        case <-ctx.Done():
            // Subscription cancelled; skip
        }
    }()

    return nil
}

func (p *Processor) EventHandlers() *eventsourcing.EventGroupProcessor {
    return eventsourcing.NewEventGroupProcessor(
        eventsourcing.OnEvent(p.OnWalletMonthClosed),
    )
}
```

**`main.go`:** see "Wiring in `main.go`" above.

**Spec (`processor_test.go`)** — "spec: Wallet Freezing — issues `FreezeWallet` when the month is
closed": given `WalletMonthClosed(walletId=…-0001)`, when the processor handles it, then
`FreezeWallet(walletId=…-0001)` is issued. The idempotency spec ("issuing it twice produces a
single `WalletFrozen`") belongs to `decide`, not here.

---

## Checklist

- [ ] `processor.go` in the same package as the command; handler injected via `NewProcessor`
- [ ] One `On<TriggerEvent>` method per trigger event; every one registered with `OnEvent`
- [ ] Handlers return `nil`; failures logged, not returned
- [ ] `context.Background()` inside the goroutine; `ctx` only used for the `ctx.Done()` select
- [ ] `delay` injected at wiring, short in dev
- [ ] Target `decide` is idempotent (`return nil, nil` when already applied)
- [ ] `EventHandlers().StreamFilter()` used as `WithFilterEvents` on the Postgres bus
- [ ] `otel.WithEventTelemetry` + unique `Subscribe` name in `main.go`
- [ ] Table-driven spec asserting event → command, with a stub handler and `delay: 0`
- [ ] Work that must survive restarts uses Variant B, not a `goroutine`