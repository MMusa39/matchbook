# 0002: LMAX Disruptor pipeline with two rings per shard

**Status:** accepted

## Context

Commands arrive on many threads (gRPC handlers). They must be journaled before they are matched,
and matching output must reach several consumers (responses, market data, Kafka) without the
matching thread doing any I/O itself.

## Decision

Each shard runs two Disruptor 4 rings:

```
producers → [inbound ring, multi-producer] → JournalStage → MatchingStage → [outbound ring, single producer] → listeners
```

- **Inbound ring:** gRPC threads claim slots and copy the command in. `JournalStage` appends to the
  journal; `MatchingStage` is gated on the journal stage's sequence, so nothing is matched before
  it is journaled. With `FsyncPolicy.EVERY_BATCH` the journal is flushed at the end of each batch
  before the sequence advances: group commit.
- **Outbound ring:** the matching thread is the only producer. It copies each engine event into a
  pre-allocated slot tagged with its command's context (correlation id, ingress time, trace ids).
  Listeners consume in parallel, each at its own pace.
- **Control messages** (snapshot, book image) travel through the inbound ring as special slots, so
  they happen at an exact point in the command stream.
- The **last event of every command** is flagged (`lastOfCommand`) by holding back publication by one
  event. That gives listeners a command boundary without an extra event.

## Consequences

- Hand-offs are pre-allocated, bounded and batch naturally under load.
- A full outbound ring back-pressures the matching thread, which in turn back-pressures producers.
  `tryPublish` lets the gateway reject with "busy" instead of blocking.
- A single-producer ring must really have one producer. Recovery replay runs inside
  `MatchingStage.onStart()`, on the matching thread, for this reason: replaying from the
  constructor thread tripped Disruptor's single-producer assertion in tests. Scheduled snapshots
  require a multi-producer inbound ring, and config validation enforces it. A test caught both
  problems.
- Disruptor threads need dedicated cores to get the latency benefit (ADR-0009).

## Alternatives considered

- **`ArrayBlockingQueue` / `LinkedBlockingQueue`:** lock-based, and the linked variant allocates per
  element.
- **Agrona `ManyToOneConcurrentArrayQueue` with a hand-written loop:** a fine choice, but the
  Disruptor gives staged gating (journal before matching), batching and multicast to listeners out of
  the box.
- **Single ring with the journal as a parallel consumer:** lower latency, but then a command could
  be matched and acknowledged before it is durable.
