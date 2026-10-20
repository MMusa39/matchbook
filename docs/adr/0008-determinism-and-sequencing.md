# 0008: Determinism and sequence numbers on every event

**Status:** accepted

## Context

Recovery by replay, golden-file tests, and debugging a production incident by replaying its journal
all require that the same input produces the same output.

## Decision

- **Pure function of its input:** `engine-core` is a pure function of `(config, command sequence)`.
  It reads no clock (timestamps come in on the command), uses no randomness, and never iterates a
  hash map to produce output. ArchUnit forbids `java.time`, `System`, `Random`, `java.util.concurrent`
  and I/O in the package.
- **Sequence numbers:**
  - every event carries a per-shard `seq`, contiguous across event types;
  - market-data events also carry a per-symbol `symbolSeq`, so a feed consumer can detect gaps per
    symbol;
  - order ids and trade ids are sequential per shard.
- **Journal numbering:** every command gets a journal sequence number; snapshots record the last one
  they include.

## Verification

- The golden-file scenarios fix exact outputs, sequence numbers included.
- A jqwik property replays random command lists twice and compares the output.
- Recovery tests compare post-recovery state byte-for-byte, and compare the replayed event stream
  with the original.
