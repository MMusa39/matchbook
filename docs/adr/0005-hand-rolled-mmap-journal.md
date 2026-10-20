# 0005: Hand-rolled memory-mapped journal (FFM) instead of Chronicle Queue

**Status:** accepted

## Context

Every command must be durable, in order, before it is matched, so that the engine can be rebuilt by
replay. Options: Chronicle Queue (an excellent production library), or a small purpose-built
append-only file.

## Decision

A hand-rolled journal (`Journal`, `JournalReader`, about 400 lines):

- **Segments:** pre-sized files named by their first sequence number, mapped with
  `FileChannel.map(..., Arena)`, which returns a Java 22+ FFM `MemorySegment`.
- **Records:** fixed 80 bytes: `[int length][int crc32c][72-byte command]`. The length is written
  last, so a record torn by a process crash reads as end-of-journal. A record torn by an OS crash
  fails its CRC, and replay stops there.
- **Durability** is configurable (`FsyncPolicy`):
  - `NONE` relies on the page cache, which survives `kill -9` of the engine (proven by
    `KillNineRecoveryTest`).
  - `EVERY_BATCH` forces the written range at the end of each Disruptor batch (group commit).
- **Pretouch** (optional) pre-faults every page of a new segment, so the journal thread never takes
  a page fault mid-command. A first single run suggested p99 at 100k/s went from 36 to 9 µs. In
  three repetitions per configuration (RESULTS §3D) that gain is within this laptop's noise, so no
  latency improvement is claimed for it.
- **Segment preparation:** a background thread creates, maps and pre-faults the next segment, and
  unmaps the full one. Rolling a 1 GiB segment synchronously stalled the journal thread for over
  100 ms, back-pressured the whole pipeline, and pushed p99 at 1M commands/s into milliseconds.
  RESULTS §3 has the before/after.

## Why not Chronicle Queue

- **Explainability:** every byte of this format can be explained in an interview. Chronicle's
  format, roll cycles and tailers are much broader than the need here.
- **Java 25:** Chronicle relies on internal JDK APIs and needs a set of `--add-opens`/`--add-exports`
  flags that change between JDK releases.
- **Same idea, no `Unsafe`:** an FFM `MemorySegment` gives the same zero-copy, syscall-free append that
  Chronicle gets from `Unsafe`. The mapping is unmapped deterministically when the arena closes.

## Related: no Agrona buffers

Agrona 2.x `UnsafeBuffer`/`ExpandableArrayBuffer` need
`--add-exports java.base/jdk.internal.misc=ALL-UNNAMED` on JDK 25 (they failed with
`IllegalAccessError` in our tests). Rather than spread that flag to every JVM, the snapshot codec uses
`ByteBuffer` and the journal uses FFM. Agrona is used only for primitive collections, which need no
flags.

## Consequences

- No replication: a single-node journal. A real venue replicates the journal to a hot standby before
  acknowledging (DESIGN.md, "At real scale").
- Segments are capped at 2 GiB because the CRC is computed through a `ByteBuffer` view.
- On macOS, `msync` (what `MemorySegment.force()` uses) does not guarantee the drive's write cache is
  flushed (Apple requires `F_FULLFSYNC` for that), so fsync numbers measured on the Mac understate
  the true cost of durability.
