# Architecture Decision Records

Each record covers one decision: the context, what was decided, what was given up for it, and the
alternatives that were considered. Numbers quoted here come from runs recorded in
[`docs/RESULTS.md`](../RESULTS.md).

| # | Decision |
|---|---|
| [0001](0001-single-threaded-matching.md) | Single-threaded matching per symbol shard |
| [0002](0002-lmax-disruptor-pipeline.md) | LMAX Disruptor pipeline with two rings per shard |
| [0003](0003-fixed-point-arithmetic.md) | Fixed-point `long` prices and quantities |
| [0004](0004-order-book-data-structures.md) | Price ladder, occupancy bitmap, intrusive queues, primitive order index |
| [0005](0005-hand-rolled-mmap-journal.md) | Hand-rolled memory-mapped journal (FFM) instead of Chronicle Queue |
| [0006](0006-snapshots-and-recovery.md) | In-stream snapshots plus journal replay for recovery |
| [0007](0007-object-pooling.md) | Object pooling for a zero-allocation hot path |
| [0008](0008-determinism-and-sequencing.md) | Determinism and sequence numbers on every event |
| [0009](0009-wait-strategies-and-affinity.md) | Configurable wait strategies; CPU pinning on Linux only |
| [0010](0010-grpc-and-rest.md) | gRPC as the primary order-entry protocol, REST for convenience |
| [0011](0011-redis-rate-limiting.md) | Per-client token-bucket rate limiting in Redis with a Lua script |
| [0012](0012-idempotent-order-submission.md) | Idempotent order submission keyed by client order id |
| [0013](0013-transactional-outbox.md) | Transactional outbox and idempotent consumers in post-trade |
| [0014](0014-market-data-snapshot-incremental.md) | Market data: sequenced incrementals with snapshot recovery |
| [0015](0015-observability.md) | Micrometer, Prometheus, Grafana-as-code and OpenTelemetry |
| [0016](0016-module-layout.md) | Maven module layout |
