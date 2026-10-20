# 0007: Object pooling for a zero-allocation hot path

**Status:** accepted

## Context

Allocation on the JVM is cheap (a TLAB pointer bump), but garbage is not free: young collections
pause application threads, and allocation churns caches. For an engine whose budget is tens of
nanoseconds per command, the goal is **no allocation at all in steady state** on the matching thread.

## Decision

- **`Order` objects** come from a pre-allocated `OrderPool` (an intrusive free list). When the pool is
  empty, new orders are rejected (`ORDER_CAPACITY_EXCEEDED`) rather than the pool growing.
- **Price levels** are pre-allocated for the whole band.
- **Events** are emitted through reusable flyweights (`ExecutionReport`, `TradeEvent`, ...) and copied
  into pre-allocated ring slots.
- **The order index** is a primitive-keyed map sized up front (ADR-0004).
- **Enums** are decoded through cached arrays; `values()` is never called on the hot path, because
  it clones.
- **Verification:** JMH with `-prof gc`. `EngineConfig.pooling(false)` switches to allocate-per-order
  so the difference can be measured, and a property test checks both modes produce identical
  output.

## Measurements (RESULTS §2)

| Benchmark | Pooled | Unpooled |
|---|---|---|
| Mixed workload | 34.8 ns, ≈0 B/op | 35.5 ns, 58 B/op |
| Add + cancel | 18.5 ns, ≈0 B/op | 24.5 ns, 56 B/op |

The mean barely moves: TLAB allocation is cheap. What pooling buys shows up in the tail under load.
In the open-loop test at 1M commands/s with G1 (RESULTS §3C), p99 is **55 µs pooled vs 1.47 ms
unpooled** (median of 3 runs). JMH's error bars are also tighter (±0.2 ns vs ±2–6 ns), and unpooled
runs allocate 1.4–2.2 GB/s.

## Consequences

- Capacity must be sized up front, and the pool's memory is committed even when idle.
- Pooled objects must be reset carefully. A stale `prev`/`next` would corrupt a queue; the
  property tests check queue integrity after every command.
