# 0001: Single-threaded matching per symbol shard

**Status:** accepted

## Context

A matching engine has to apply orders to a book in a single, well-defined order: price-time
priority is defined in terms of arrival order. Concurrency inside one book needs locks or lock-free
structures around every level and order, which is hard to get right and makes latency depend on
contention. Real venues (LMAX, most exchange cores) run one thread per book or per group of books.

## Decision

Each **shard** owns a set of symbols and exactly one matching thread, which is the only thread that
reads or writes those books (`MatchingEngine` is deliberately not thread-safe). Symbols are assigned
to shards by configuration. Order and trade ids carry the shard id in their top 15 bits
(`EngineConfig.SHARD_SHIFT`), so ids are unique across shards without coordination.

## Consequences

- No locks, no CAS and no memory barriers on the matching path. The code reads like a textbook
  order book, and its cost is predictable: the mixed-workload benchmark measures about 35 ns per
  command (RESULTS §2).
- Determinism comes for free: one thread, one input order, one output order (ADR-0008).
- Throughput per symbol is bounded by one core. Scaling out means more shards, not more threads per
  book. A single very hot symbol cannot be split.
- Cross-symbol operations (spreads, a portfolio-level risk check inside the engine) would need
  coordination between shards; this project keeps risk in the gateway (ADR-0010).

## Alternatives considered

- **Lock per book, thread pool:** simpler hand-off, but every order pays for lock acquisition, and
  fairness and latency depend on the scheduler.
- **Lock-free book:** much harder to verify, and still serialises on the hot price levels.
- **Actor per symbol:** equivalent to this design, with the mailbox being the ring buffer; the
  Disruptor gives a bounded, pre-allocated, batching mailbox (ADR-0002).
