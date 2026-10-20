# Design

How Matchbook works inside: the order book, the threads, how state survives a crash, what fails and
how, and what would change at a real exchange's scale. Decisions and their alternatives are in
[`adr/`](adr/README.md); measurements are in [`RESULTS.md`](RESULTS.md).

## 1. Order book

### Data structures

```
OrderBook (one per symbol)
├── bids: PriceLevel[levels]   index = (price − lowPrice) / tick, levels pre-allocated for the whole band
├── asks: PriceLevel[levels]
├── bidLevels, askLevels: BitSet   bit i set ⇔ level i is non-empty
├── bestBid, bestAsk: int          cached ladder index of the best level (−1 if empty)
└── symbolSeq                      per-symbol market data sequence

PriceLevel
├── head ⇄ order ⇄ order ⇄ … ⇄ tail    intrusive doubly linked FIFO through Order.prev / Order.next
├── totalQuantity, orderCount
└── price, index

MatchingEngine (one per shard)
├── books: OrderBook[symbolId]
├── ordersById: Agrona Long2ObjectHashMap<Order>   pre-sized; never rehashes
├── pool: OrderPool                                 pre-allocated orders, intrusive free list
└── flyweights: ExecutionReport, TradeEvent, BookLevelUpdate, TopOfBook
```

### Complexity

| Operation | Cost | Why |
|---|---|---|
| Rest a limit order | O(1) | array index + list append + bit set |
| Cancel by order id | O(1) expected | hash lookup + unlink; O(L/64) words scanned only if the best level emptied |
| Replace, quantity down | O(1) | amended in place, keeps priority |
| Replace, price change or quantity up | O(1) + matching | unlink, re-enter at the back |
| Match against k resting orders over m levels | O(k + m + gap/64) | walk the head of each level; bitmap scan to the next level |
| Top of book | O(1) | cached best index + level totals |
| FOK feasibility | O(k) | walks the orders it would fill, read-only, before touching anything |
| Snapshot | O(resting orders + levels/64) | levels in price order, orders in queue order |

L is the number of ladder levels (band width / tick). Memory is O(L) per symbol for the ladder, plus
O(maxOrders) for the pool and the index, all allocated at start-up.

### Rules

- **Priority:** price, then time (arrival order within the level). Trades happen at the resting order's
  price.
- **Market orders** must be IOC or FOK. A market order's unfilled remainder is cancelled
  (`IOC_REMAINDER`).
- **FOK** checks feasibility first, without side effects. A killed FOK leaves the book exactly as it
  was. Self-trade prevention is accounted for: under `CANCEL_RESTING` the walk skips own orders, and
  under the other modes it stops at the first one.
- **Cancel/replace:** a quantity decrease at the same price keeps queue position. Any price change or
  quantity increase loses it. Quantity uses FIX `OrderQty` semantics (the new total including filled
  quantity), and a replace to at or below the filled quantity is rejected. A replace that crosses the
  spread trades immediately.
- **Self-trade prevention** is per incoming order: `CANCEL_RESTING` (default), `CANCEL_AGGRESSOR` or
  `CANCEL_BOTH`. Two orders self-match when they have the same client id.
- **Validation:** tick, lot, min/max quantity, the static price band, and pool capacity. A reject
  consumes no order id.
- **Information hiding:** cancel or replace of another client's order is reported as `UNKNOWN_ORDER`.

### Event order per command

The first response comes first (`NEW`, `REJECTED`, `CANCELED`, `CANCEL_REJECTED`, `REPLACED`,
`REPLACE_REJECTED`). Then, per fill: `TRADE`, the maker's report, the taker's report. Then one level
update per touched level, then the remainder cancel if any, then top of book if it changed. Every
event gets the next shard-wide `seq`; public events also get the next `symbolSeq`. The golden-file
scenarios under `engine-core/src/test/resources/scenarios` show exact streams.

## 2. Threading model

```
gRPC threads ─┐                     ┌─ PendingResponses ──▶ unary Submit responses
load gen ─────┼▶ inbound ring ─▶ journal ─▶ matching ─▶ outbound ring ─┼─ EventStreams ─────▶ gateways (gRPC stream)
snapshot timer┘  (multi-prod.)   thread     thread    (single prod.)   ├─ MarketDataStreams ▶ market data (gRPC stream)
                                                                        ├─ KafkaEventPublisher ▶ Kafka
                                                                        └─ EngineMetrics ─────▶ Micrometer
```

**Per shard: two Disruptor rings and 2 + n threads.**
- The **journal thread** appends each command to the memory-mapped journal and, under
  `FsyncPolicy.EVERY_BATCH`, forces the batch to disk.
- The **matching thread** is the only thread that touches the engine. It is gated on the journal
  thread's sequence, so nothing is matched before it is journaled.
- **Each outbound listener has its own thread** and sees every event in order. A slow listener delays
  only itself, until the 65 536-slot outbound ring is full. Then it back-pressures the matching
  thread, which back-pressures producers. `tryPublish` lets the gRPC front answer `RESOURCE_EXHAUSTED`
  instead of blocking.
- **One background thread per journal** prepares the next segment (create, map, pre-fault) and
  releases the full one (unmap). Before this existed, a synchronous roll of a 1 GiB segment stalled the
  journal thread for over 100 ms (RESULTS §3).

**Memory visibility:** hand-offs go through Disruptor sequences, which are release/acquire, so ring
slots are plain mutable objects. Metrics counters written by the matching thread are plain `long`s
read by others: staleness is acceptable there, and the hot path has no barriers.

**Allocation:**
- **Matching thread:** none in steady state. JMH `-prof gc` shows ≈0 B/op for every workload. Pooling
  can be switched off for comparison.
- **Journal thread:** none per command (FFM segment writes, a reused CRC32C).
- **Listener threads** allocate Protobuf messages and Kafka records. That is deliberate: it is off the
  critical path, and listeners can fall behind without affecting matching.

**Wait strategies** are configurable per ring: `BUSY_SPIN`, `YIELDING`, `SLEEPING`, `BLOCKING`
(ADR-0009, RESULTS §3B). Benchmarks use busy-spin. Docker and Kubernetes default to sleeping inbound
and blocking outbound, because spinning threads on a 4-vCPU Docker VM would starve everything else.

## 3. Durability and recovery

**Journal:** 80-byte records `[length][crc32c][command]`, length written last, in pre-sized segment
files named by first sequence. See ADR-0005 for the format and its failure modes:
- a torn write reads as end-of-journal;
- a CRC mismatch stops replay;
- damage before the end of the journal refuses start-up;
- a journal behind its snapshot is moved aside.

**Snapshots** are control slots in the inbound ring, so each one sits at an exact journal sequence:
1. The matching thread serialises the engine into memory (a canonical byte layout).
2. A background thread writes it to a temporary file, forces it, and atomically renames it.
3. Old snapshots and journal segments are pruned, but never past the *replay floor*.

**Recovery** runs on the matching thread before the pipeline accepts input:
1. Load the newest valid snapshot at or below the replay floor.
2. Replay the journal after it. Replayed events go to the listeners again, with correlation id 0.
3. Start.

The *replay floor* is the highest journal sequence Kafka has fully acknowledged (ADR-0013). It is what
makes the journal the engine's outbox: everything Kafka might be missing is re-sent after a restart.

**Proof:**
- `EnginePipelineTest` compares post-recovery state byte-for-byte with the pre-crash state.
- `KillNineRecoveryTest` SIGKILLs a child JVM mid-stream and compares recovered state with a fresh
  engine fed exactly the journaled commands.
- `EngineSnapshotTest` (jqwik) snapshots at random points and checks the restored engine behaves
  identically to the original for the rest of the sequence.

## 4. Gateway, market data, post-trade

- **Gateway:** auth (API key → client id), then one Redis Lua call (idempotency, token bucket, internal
  id), then risk (size, notional, fat finger, position including open orders and in-flight
  reservations), then the engine (gRPC deadline + circuit breaker, no retries). The outcome is stored
  under the client order id. Risk state comes from the engine's event stream. REST, gRPC and FIX 4.4
  (QuickFIX/J) share one `OrderService`. FIX sessions authenticate Logon by API key, and receive
  execution reports from the engine stream, including fills of resting orders.
- **Market data:** a replica per symbol from in-stream images plus sequenced incrementals. WebSocket
  clients get a snapshot at `n`, then exactly `n+1, n+2, …`. A gap triggers a stale notice and an
  automatic resync (ADR-0014).
- **Post-trade:** an idempotent consumer keyed on `(shard, seq)` and positions with average-cost P&L,
  in one Postgres transaction per Kafka poll together with the outbox inserts. That is 3.7× faster to
  drain a backlog than one transaction per record (RESULTS §6). Also a `SKIP LOCKED` relay, and a DLT
  for poison records, which are identified by their index in the batch (ADR-0013).

## 5. Failure modes

| Failure | What happens | Recovery |
|---|---|---|
| Engine process crash / `kill -9` | Journal in the page cache is intact. In-flight gRPC calls fail; the gateway marks them `UNKNOWN` (no blind retry) | Restart: snapshot + replay; Kafka gets the missing events again; consumers dedupe |
| Power loss with `FsyncPolicy.NONE` | The journal tail since the last OS writeback is lost | Recovery uses what is on disk; if the snapshot is ahead of the journal, a new journal starts after it. Use `EVERY_BATCH` (or replication) when this matters |
| Journal damage mid-file | Start-up refuses | Operator: restore from a replica or backup |
| Kafka down | The acked floor stops; the producer buffers, then blocks its listener; the outbound ring fills; the engine stops taking orders (`RESOURCE_EXHAUSTED`, gateway 503, circuit opens) | Kafka returns, the backlog drains. Deliberate: no trading without the drop copy |
| Redis down, or full | The gateway rejects all orders `503 REDIS_UNAVAILABLE` (fail closed). A full Redis (`noeviction`) behaves the same; this was observed in a load test before the idempotency window was sized (ADR-0012) | Automatic when Redis returns, or once keys expire |
| Engine slow or down, as seen from the gateway | Deadline exceeded → `UNKNOWN`; repeated failures open the circuit → fast `503` | Half-open probes close it |
| Postgres down | Post-trade retries with backoff, then DLT; Kafka offsets are not committed, so nothing is lost | Redeliver or replay the DLT |
| Slow market data client | Its bounded queue overflows; it is disconnected | Reconnect → fresh snapshot |
| Market data gap | Replica marked stale, clients told, engine stream re-subscribed | Fresh images |
| Gateway restart | Risk state is rebuilt from the event stream from "now"; open orders placed before the restart are not counted until their next report | Limitation, below |

## 6. Known limitations and what I would do at real exchange scale

- **Replication.** The journal lives on one node. A real venue sequences input through a replicated
  log (Raft, or Aeron Cluster) and acknowledges an order only once a quorum has it. A hot standby
  replays the same log, so failover takes milliseconds and nothing is lost. This is the biggest gap.
- **Transport.** gRPC over TCP costs tens of microseconds per hop. Production order entry uses binary
  protocols over TCP (OUCH/BOE-style, or FIX/SBE). Market data goes out over UDP multicast with gap fill
  and snapshot recovery channels. Engine I/O uses kernel bypass (Solarflare/OpenOnload, DPDK), and the
  journal goes over Aeron.
- **Hardware and OS.** Isolated, pinned cores (`isolcpus`, `nohz_full`), IRQ steering, huge pages,
  turbo off, NUMA-local memory. None of it is possible on the development laptop: this is why the
  laptop's p99.9 and above are dominated by the OS (RESULTS §3).
- **Order index.** Encoding the pool slot in the order id would turn cancel lookups into an array
  access with no hashing (ADR-0004 shows `HashMap` beating Agrona on mean time for this access pattern).
- **Risk.** Gateway risk is per instance and rebuilt from "now" after a restart. Production would seed
  it from post-trade positions plus the engine's open-order state, or run risk as a sequenced stage
  inside the engine pipeline.
- **Market data.** Each WebSocket snapshot is JSON built per client. Real feeds pre-encode one binary
  message per update and fan out with zero copy.
- **Housekeeping.** `processed_events` and the outbox need retention tied to the replay floor, and
  the ladder needs re-centring if the price band moves intraday.
