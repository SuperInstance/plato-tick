# plato-tick — Inter-Agent Message Board

A shared message board where agents leave ticks for each other. Direct messages, broadcasts, topic subscriptions, TTL expiry, and acknowledgment tracking — the communication backbone for multi-agent coordination.

**Part of the [Plato](https://github.com/SuperInstance/plato-shell) ecosystem.**

## What This Gives You

- **Directed and broadcast ticks** — send to a specific agent or everyone
- **Topic subscriptions** — subscribe to topics, poll for new ticks
- **Priority levels** — Info, Normal, Urgent, Critical
- **TTL expiry** — ticks auto-expire after a configurable time
- **Acknowledgment tracking** — know which agents have seen and acted on a tick

## Quick Start

```rust
use plato_tick::{TickBoard, TickPriority};

let board = TickBoard::new();

// Post a broadcast tick
let tick = board.post("analyst", None, "results", "Dataset analysis complete", TickPriority::Normal, 60_000);

// Post a direct message
let tick = board.post("analyst", Some("planner"), "urgent", "Need re-prioritization", TickPriority::Urgent, 30_000);

// Subscribe and poll
let sub = board.subscribe("planner", vec!["urgent", "results"]);
let ticks = board.poll(sub);

// Acknowledge
board.acknowledge(tick.id, "planner", "Re-prioritized task queue");
```

## How It Fits

The message layer between agents. Uses [plato-a2a](https://github.com/SuperInstance/plato-a2a) types for wire format. Agents in [plato-shell](https://github.com/SuperInstance/plato-shell) communicate through ticks. [plato-observe](https://github.com/SuperInstance/plato-observe) monitors tick flow for performance analysis.

## Installation

```toml
[dependencies]
plato-tick = "0.1"
```

## License

MIT
