# 0006: In-stream snapshots plus journal replay for recovery

**Status:** accepted

## Context

Replaying the whole journal from the start of the day takes time that grows with the day. Snapshots
bound it, but a snapshot is only useful if it corresponds to an exact journal position.

## Decision

- **Taking a snapshot:** a snapshot request is a control slot in the inbound ring. When the matching
  thread reaches it, it serialises the engine (`EngineSnapshot`, a canonical byte layout) into memory,
  tagged with the last applied journal sequence. A background thread writes it to a temporary
  file, forces it to disk, and atomically renames it. Snapshots older than the newest `snapshotsToKeep`
  are deleted, and so are journal segments that are fully covered by the oldest kept snapshot.
- **Recovery:** load the newest snapshot that passes its CRC (falling back to older ones), then replay
  the journal records after it. This runs on the matching thread before the pipeline accepts input.
  Replayed events are re-published with correlation id 0; downstream consumers de-duplicate by
  `(shard, seq)` (ADR-0013).
- **Fail-fast cases:**
  - Damage before the end of the journal stops startup with an error rather than silently skipping
    commands.
  - A journal that ends *before* the snapshot (possible after power loss with `FsyncPolicy.NONE`) is
    moved aside, and a new journal starts after the snapshot.
- **Proof:** the state after recovery is compared byte-for-byte with the state before the crash
  (`EnginePipelineTest`), and with a fresh engine fed the same commands after a real `kill -9` of a
  child JVM (`KillNineRecoveryTest`).

## Consequences

- Snapshot serialisation pauses matching (measured in RESULTS §4); the file write does not.
- Snapshot size grows with the number of resting orders (76 bytes per resting order).
- Replaying re-emits events, so every consumer must be idempotent.

## Alternatives considered

- **Copy-on-write or fork-based snapshots:** no pause, much more complex.
- **Snapshot by a second replica:** replaying the journal on a follower avoids the pause entirely, and
  is what a production venue would do (DESIGN.md).
