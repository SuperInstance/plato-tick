# Dependencies — plato-tick

## Ecosystem Role

plato-tick is the **time-series and scheduling primitive** layer for the Plato subsystem. It provides tick-level ingestion, time-windowed aggregation, and scheduling primitives that power real-time monitoring, alerting, and fleet health checks across the SuperInstance ecosystem.

---

## Upstream Dependencies

| Repository | Description |
|---|---|
| [openconstruct-abi](https://github.com/SuperInstance/openconstruct-abi) | ABI types for tick data serialization |
| [plato-adapters](https://github.com/SuperInstance/plato-adapters) | Adapter interfaces for data source integration |
| [plato-construct](https://github.com/SuperInstance/plato-construct) | Construct-level orchestration primitives |

## Downstream Dependents

| Repository | Description |
|---|---|
| [openconstruct](https://github.com/SuperInstance/openconstruct) | Core framework uses tick scheduling |
| [openconstruct-rust](https://github.com/SuperInstance/openconstruct-rust) | Rust runtime implements tick evaluation |
| [plato-vision](https://github.com/SuperInstance/plato-vision) | Vision subsystem consumes tick streams |
| [fleet-health-monitor](https://github.com/SuperInstance/fleet-health-monitor) | Fleet health consumes tick metrics |
| [cocapn-core](https://github.com/SuperInstance/cocapn-core) | Co-captain core uses tick for heartbeat coordination |

## Documentation

- [OpenConstruct Docs](https://github.com/SuperInstance/openconstruct-docs)
- [SuperInstance Wiki](https://github.com/SuperInstance/superinstance-wiki)
