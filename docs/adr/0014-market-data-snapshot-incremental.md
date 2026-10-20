# 0014: Market data: sequenced incrementals with snapshot recovery

**Status:** accepted

## Context

Market-data consumers keep their own copy of the book. They need a consistent starting point, every
change after it, and a way to know when they have missed one.

## Decision

- **Engine side:**
  - every public event (trade, L2 level update, top of book) carries a per-symbol `symbolSeq`, assigned
    on the matching thread;
  - a full book image can be requested *through the inbound ring*, so the matching thread emits it at
    an exact point in the stream (`IMAGE_START … IMAGE_END` with the image's `symbolSeq`);
  - a new gRPC market-data subscriber gets an image per symbol first, then only incrementals with a
    higher sequence.
- **Market-data service:**
  - keeps a replica per symbol and applies incrementals only when `seq == last + 1`;
  - ignores duplicates (`seq <= last`);
  - on a gap, marks the replica stale, tells its WebSocket clients (`{"type":"stale"}`), and
    reconnects to the engine for fresh images.
- **WebSocket clients:** a new client gets `snapshot(seq = n)` followed by exactly `n+1, n+2, …`.
  Snapshot-and-register, and apply-and-broadcast, happen under the same per-symbol lock, so nothing
  can fall between the snapshot and the stream. Clients check the same `seq + 1` rule.
- **Slow consumers:** each WebSocket client has a bounded queue drained by its own virtual thread. A
  client that falls 10 000 messages behind is disconnected; it reconnects and receives a fresh
  snapshot. One slow reader never delays the others.

## Consequences

- Sequence numbers are per symbol, so a client tracking several symbols checks each independently.
- An image is the full depth of a symbol. Snapshots sent to WebSocket clients are capped at 50 levels
  per side; incrementals still cover every level.

## Alternatives considered

- **UDP multicast with a separate TCP snapshot and recovery channel:** what exchanges actually use (for
  example ITCH plus its recovery protocols), and the right answer at scale. It needs a multicast-capable
  network, which Docker Desktop and kind do not give us.
- **Kafka for market data:** durable and replayable, but adds milliseconds and a broker hop to a feed
  whose value decays in microseconds.
