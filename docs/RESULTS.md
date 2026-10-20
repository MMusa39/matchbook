# Results

Every number in this repository comes from a run recorded here. The raw outputs are committed under
[`results/`](results/), and each section gives the command that reproduces it.

## 1. Environment

| | |
|---|---|
| Machine | Apple M5 laptop (4 performance + 6 efficiency cores), 16 GB RAM, macOS 26.6.2, on AC power |
| JVM | OpenJDK 25.0.4.1 (Homebrew build), HotSpot, default G1 unless stated |
| Heap | `-Xms2g -Xmx2g -XX:+AlwaysPreTouch` (JMH and load generator) |
| Background load | VS Code, Chrome (playing video) and a Colima Linux VM were running; normal for a laptop, and the reason for repeated runs |
| Isolation | **None.** macOS cannot pin threads or isolate cores. Spinning threads compete with everything else |
| Code | JMH: `engine-core` at `7efd952`. Open-loop matrix and recovery timer: `df5f360`. Snapshot timer: `c9c5294`. Journal-roll comparison: `95828d6` vs `15afe54`. Gatling: `c9c5294`+ with the simulation fix. Post-trade drain: `8cf1271` |

**How to read the latency numbers.** Medians and p99 are stable and characterise the engine. p99.9
and above, and the maxima, are dominated by the OS descheduling threads: in every open-loop run the
load generator's own lag behind its schedule (`producer lag`) is of the same size as the maximum
latency, and macOS offers no way to prevent it. Comparisons between configurations are only
meaningful where the ranges across repetitions do not overlap.

## 2. Matching engine, single thread (JMH)

```bash
./mvnw -q install -DskipTests
java -jar engine-bench/target/benchmarks.jar MatchingEngineBenchmark -prof gc -rf json -rff docs/results/jmh-engine.json
```

JMH 1.37, average time, 3 forks × 5 measurement iterations of 2 s (5 × 1 s warm-up), one thread, no
pipeline and no I/O. Each score is **per engine command**. Raw output:
[`jmh-engine.txt`](results/jmh-engine.txt), [`jmh-engine.json`](results/jmh-engine.json).

| Benchmark | What one operation is | Pooled ns/cmd | Pooled B/cmd | Unpooled ns/cmd | Unpooled B/cmd |
|---|---|---|---|---|---|
| `mixedWorkload` | 50% passive limit, 25% cancel, 10% replace, 15% aggressive IOC against a ~3.7k-order book | **34.8 ± 0.4** | **≈ 10⁻⁴** | 35.5 ± 1.8 | 58 |
| `addThenCancel` | rest a limit order, cancel it | **18.5 ± 0.2** | **≈ 10⁻⁴** | 24.5 ± 2.2 | 56 |
| `restThenFullFill` | rest an order, fill it completely (one trade) | **75.4 ± 0.2** | **≈ 10⁻⁴** | 76.8 ± 6.4 | 112 |
| `sweepTenLevels` | ten resting orders, one market order sweeping them | **49.9 ± 0.6** | **≈ 10⁻⁴** | 57.2 ± 6.0 | 112 |

- **Zero allocation:** the pooled engine allocates nothing per command. The ≈10⁻⁴ B/op is JMH's own
  bookkeeping, spread over billions of operations. CI fails the build if `mixedWorkload` allocates 1
  byte per op or more.
- **Pooling and the mean:** pooling barely changes mean cost, because TLAB allocation is cheap. It
  narrows the error bars, and it removes 1.4–2.2 GB/s of garbage. The tail-latency effect shows up
  under load in §3C.
- **Throughput:** 34.8 ns per command in the mixed workload is about 28.7 million commands per second
  of pure matching on one core.

### Order-id index in isolation

```bash
java -jar engine-bench/target/benchmarks.jar OrderIndexBenchmark -prof gc
```

One put, one get and one remove per op, sequential ids as the engine assigns them. Raw:
[`jmh-order-index.txt`](results/jmh-order-index.txt).

| Map | 4 096 live entries | 1 000 000 live entries | Allocation |
|---|---|---|---|
| Agrona `Long2ObjectHashMap` (used) | 30.6 ± 0.2 ns | 48.6 ± 0.5 ns | ≈ 10⁻⁴ B/op |
| `java.util.HashMap<Long, Object>` | 16.2 ± 0.3 ns | 16.4 ± 0.2 ns | 104 B/op |

This went against the design. For sequential `Long` keys, `HashMap` is faster on the mean: its buckets
receive keys in order and its nodes are allocated next to each other. Agrona is kept because it
allocates nothing (ADR-0004). The better option, encoding the pool slot in the order id so no hashing
is needed, is listed as future work.

## 3. Full pipeline under open-loop load (HdrHistogram, coordinated-omission corrected)

```bash
scripts/bench-matrix.sh            # 18 configurations × 3 repetitions × (10 s warm-up + 20 s measured)
scripts/summarize-loadgen.py       # the tables below
```

**What is measured.** The time from a command's **intended** send time to its last event reaching an
outbound listener:
- the inbound ring hand-off (single producer)
- journal append (memory-mapped, CRC32C)
- matching
- the outbound ring hand-off to a consumer thread

The generator sends on a fixed schedule (`start + i × interval`). If the system stalls, commands that
should have been sent during the stall are charged the time they waited. See §5 for why this matters.

Unless stated otherwise: the inbound stages use `BUSY_SPIN`, the measuring listener uses `YIELDING`,
pooling is on, journal pretouch is on, fsync is `NONE`, and G1 is the collector. Every cell is the
**median of 3 runs, with the [min–max] range**. Values are µs unless the column says otherwise. Raw
per-run output, and `.hgrm` percentile distributions, are in [`results/loadgen/`](results/loadgen/).

### A. Rate sweep

| config | p50 µs | p90 µs | p99 µs | p99.9 µs | p99.99 µs | max µs | producer lag ms |
|---|---|---|---|---|---|---|---|
| rate-100000 | 0.75 [0.75–0.75] | 1.00 [1.00–1.04] | 24.5 [19.8–56.6] | 5652 [3762–7524] | 11002 [9429–12853] | 12976 [11387–14795] | 7.8 [7.4–7.8] |
| rate-250000 | 0.71 [0.71–0.75] | 0.96 [0.92–1.00] | 24.1 [18.4–29.7] | 5218 [4465–5878] | 10846 [10273–11575] | 12476 [11108–13394] | 8.3 [6.2–8.3] |
| rate-500000 | 0.67 [0.67–0.67] | 1.00 [1.00–1.00] | 88.8 [43.7–262.4] | 8159 [6111–9658] | 11575 [11035–13263] | 13361 [12567–14049] | 10.1 [9.7–18.3] |
| rate-1000000 | 0.63 [0.63–0.67] | 1.08 [1.08–1.08] | 50.9 [45.3–67.1] | 6988 [6463–7107] | 11543 [11395–11575] | 13214 [12214–13238] | 16.1 [16.0–21.9] |
| rate-2000000 | 0.67 [0.67–0.71] | 3.63 [3.25–7.30] | 235.9 [187.0–3559.4] | 8552 [5186–12419] | 12681 [7741–15376] | 13402 [8454–16466] | 30.3 [25.3–32.4] |

- Up to 2M commands/s, the median stays at **0.6–0.75 µs**.
- Up to 1M commands/s, p99 stays at **24–89 µs**.
- At 2M/s, p90 rises (3.6 µs) and p99 jumps to 236 µs, with one run at 3.6 ms. That is where queues
  begin to form behind the busy threads on this machine.

### B. Wait strategies (200k commands/s)

| config | p50 µs | p90 µs | p99 µs | p99.9 µs | p99.99 µs | max µs | CPU cores |
|---|---|---|---|---|---|---|---|
| wait-BUSY_SPIN | 0.75 [0.71–0.75] | 0.96 [0.96–1.00] | 23.8 [21.8–27.2] | 6066 [3791–6169] | 10551 [6423–11633] | 11788 [7574–12714] | 3.97 [3.96–3.97] |
| wait-YIELDING | 0.83 [0.79–0.83] | 2.08 [1.88–2.29] | 57.0 [29.1–58.5] | 10928 [5308–14778] | 18186 [10789–19104] | 20038 [12730–20054] | 3.94 [3.93–3.95] |
| wait-SLEEPING | 0.83 [0.83–0.83] | 2.13 [2.00–2.25] | 31.7 [27.6–35.3] | 1820 [836–1936] | 6615 [5349–9110] | 7950 [6242–10076] | 3.93 [3.93–3.95] |
| wait-BLOCKING | 6.25 [6.21–6.42] | 10.75 [10.63–10.88] | 43.9 [36.1–47.5] | 3946 [122–4735] | 9904 [6521–14500] | 11829 [8049–16171] | 2.71 [2.71–2.72] |

- **Busy-spin** gives the lowest median and p99.
- **Blocking** costs about **6 µs at the median**, because every hand-off can wake a parked thread.
  It is the only option that saves CPU (2.7 cores instead of ~3.9).
- **Sleeping** had the *best* p99.9 on this laptop. Fewer threads spinning gives the OS room to run
  everything else without preempting the engine's threads. That is a laptop artefact rather than a
  property of the strategy.
- The CPU column is the whole process, including the generator's own busy-spinning producer thread.

### C. Object pooling × garbage collector (1M commands/s)

| config | p50 µs | p90 µs | p99 µs | p99.9 µs | p99.99 µs | max µs | GCs | GC ms |
|---|---|---|---|---|---|---|---|---|
| gc-G1-pooling-true | 0.67 [0.67–0.71] | 1.13 [1.13–1.21] | 54.9 [41.0–85.5] | 8327 [7602–9421] | 12616 [11821–13263] | 13517 [13132–14402] | 0 [0–0] | 0 [0–0] |
| gc-G1-pooling-false | 0.71 [0.71–0.75] | 1.63 [1.54–3.67] | 1471.5 [341.8–9437.2] | 31752 [8790–66454] | 37683 [14270–71107] | 38928 [15753–71631] | 2 [2–2] | 9 [8–9] |
| gc-Z-pooling-true | 0.63 [0.63–0.63] | 1.25 [1.13–1.67] | 749.1 [521.2–993.3] | 11682 [9118–16048] | 17400 [16482–23429] | 18006 [17711–25068] | 0 [0–0] | 0 [0–0] |
| gc-Z-pooling-false | 0.63 [0.63–0.63] | 2.88 [1.21–4.42] | 960.0 [166.3–2512.9] | 10945 [9093–11747] | 15401 [12575–16712] | 15983 [13189–18121] | 18 [18–24] | 123 [115–145] |
| gc-Epsilon-pooling-true | 0.58 [0.58–0.63] | 1.08 [0.92–1.08] | 130.1 [38.0–294.1] | 8897 [7180–9839] | 12509 [11543–13279] | 13435 [12935–14746] | 0 [0–0] | 0 [0–0] |

- **G1, pooling on vs off:** p99 is **55 µs vs 1.47 ms** (median of 3 runs; the unpooled range is
  0.34–9.4 ms). This is the tail-latency cost of allocation that JMH's mean could not show.
- **ZGC with pooling on:** a worse p99 (749 µs) than G1, with zero collections. ZGC's load barriers
  and concurrent threads add work, and its threads compete with four spinning threads for four
  performance cores.
- **Epsilon** (no GC at all) matches G1 with pooling on. With zero allocation, the collector is
  irrelevant.
- **The honest summary:** zero allocation is what made GC choice irrelevant. Picking a "better"
  collector does not substitute for it.

### D. Journal page pre-faulting (200k commands/s)

| config | p50 µs | p90 µs | p99 µs | p99.9 µs | p99.99 µs | max µs |
|---|---|---|---|---|---|---|
| pretouch-true | 0.75 [0.71–0.75] | 0.96 [0.96–1.00] | 23.4 [16.9–24.9] | 6738 [3756–7578] | 13099 [8184–13820] | 14320 [9019–15761] |
| pretouch-false | 0.71 [0.71–0.75] | 0.96 [0.92–1.04] | 18.0 [17.4–33.3] | 2044 [1393–5931] | 7045 [6840–10412] | 8405 [8061–12165] |

An earlier single run showed pretouch cutting p99 from 36 µs to 9 µs. With three repetitions each, the
ranges overlap, and the effect **does not reproduce** above this machine's noise. Pretouch stays on by
default because it removes page-fault work from the journal thread, but no latency gain is claimed for
it.

### E. Durability: fsync per batch (100k commands/s)

| config | p50 µs | p90 µs | p99 µs | p99.9 µs | p99.99 µs | max µs | CPU cores |
|---|---|---|---|---|---|---|---|
| fsync-NONE | 0.75 [0.75–0.75] | 1.04 [1.00–1.04] | 29.2 [18.0–165.2] | 8110 [4342–8237] | 11493 [8421–12534] | 13402 [9650–13681] | 3.96 [3.96–3.97] |
| fsync-EVERY_BATCH | 29.38 [28.88–29.50] | 39.78 [38.88–40.35] | 72.6 [68.2–82.8] | 5411 [219–6140] | 9609 [4358–12378] | 11133 [5992–13935] | 3.10 [3.10–3.13] |

- **The cost:** forcing the journal to the device at the end of every Disruptor batch (group commit)
  raises the median from **0.75 µs to 29 µs**. That is the cost of one `msync` per batch on the
  laptop's SSD.
- **macOS caveat:** `msync` on macOS does not guarantee that the drive's own cache is flushed (that
  needs `F_FULLFSYNC`), so this understates the true cost of power-loss durability.
- **Why it isn't the default:** a production venue gets durability from replication rather than
  synchronous local flushes (DESIGN.md §6).

### Journal segment roll: before and after (1M commands/s)

Each run journals 30 M commands, so the 1 GiB journal segment (13.4 M records) rolls during
measurement. Before the fix, rolling mapped and pre-faulted the new 1 GiB file, and unmapped the old
one, synchronously on the journal thread. The inbound ring filled behind it, and the producer blocked
on a full ring: in one diagnostic run (console output, not kept), a single `publish` blocked for 141 ms. JFR showed a
deoptimisation of `Journal.append` at the roll branch at the same moment. After the fix, a background
thread prepares the next segment and releases the old one, so a roll is a rename and a pointer swap.

Both builds ran interleaved (before, after, before, …) in the same session, 3 runs each at 1 M commands/s:

```bash
git worktree add /tmp/before 95828d6 && (cd /tmp/before && ./mvnw -q install -DskipTests -pl engine-bench -am)
java -Xms2g -Xmx2g -XX:+AlwaysPreTouch -cp <jar> dev.matchbook.bench.OpenLoopLoadGenerator \
     --rate 1000000 --duration 20 --warmup 10 --wait BUSY_SPIN --outbound-wait YIELDING --label journal-roll-<v>-rN
```

| journal | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs | producer lag ms |
|---|---|---|---|---|---|---|
| synchronous roll (`95828d6`) | 0.63 [0.58–0.63] | 0.88 [0.88–1.00] | 104792.1 [14016.5–174850.0] | 218890 [109969–314311] | 235012 [125567–330301] | 221.3 [112.6–316.9] |
| background prepare/release (`15afe54`) | 0.67 [0.67–0.67] | 0.88 [0.88–0.92] | 22.9 [22.4–64.1] | 3604 [2263–4911] | 12141 [9994–13148] | 19.0 [17.1–21.5] |

p99 at 1 M commands/s drops from **105 ms to 23 µs** (median of 3). Raw: `results/loadgen/journal-roll-*.txt`.

## 4. Recovery and snapshots

```bash
java -Xms2g -Xmx2g -XX:+AlwaysPreTouch -cp engine-bench/target/benchmarks.jar dev.matchbook.bench.RecoveryTimer --commands 10000000
java -Xms2g -Xmx2g -XX:+AlwaysPreTouch -cp engine-bench/target/benchmarks.jar dev.matchbook.bench.RecoveryTimer --commands 10000000 --snapshot-at 9000000
java -Xms4g -Xmx4g -cp engine-bench/target/benchmarks.jar dev.matchbook.bench.SnapshotPauseTimer 7
```

**Restart time.** The journal holds 10 000 000 synthetic commands (800 MB). Each case was timed over
5 runs; the journal files are in the OS page cache (a warm restart). Raw:
[`recovery-journal-only.txt`](results/loadgen/recovery-journal-only.txt),
[`recovery-snapshot.txt`](results/loadgen/recovery-snapshot.txt).

| Case | `EnginePipeline.start()` median | Runs (ms) | Replay rate |
|---|---|---|---|
| No snapshot: replay all 10M commands | **1 101 ms** | 1629, 1076, 1056, 1101, 1115 | 11.3 M commands/s |
| Snapshot at 9M + replay 1M | **485 ms** | 648, 487, 477, 485, 471 | 3.9 M commands/s (includes snapshot load) |

**Snapshot pause.** Time the matching thread spends serialising the engine. Writing the file happens
on another thread. 7 runs per size after warm-up. Raw: [`snapshot-pause.txt`](results/snapshot-pause.txt).

| Resting orders | Snapshot size | Pause median | Range |
|---|---|---|---|
| 10 000 | 0.76 MB | **1.0 ms** | 0.46–2.59 ms |
| 100 000 | 7.6 MB | **2.8 ms** | 2.51–4.24 ms |
| 1 000 000 | 76 MB | **93 ms** | 85.7–96.7 ms |

The pause grows linearly with book size. At a million resting orders it is significant: most of it is
allocating and zeroing two 76 MB buffers per snapshot. Reusing a pre-allocated buffer, or snapshotting
on a replica instead (DESIGN.md §6), would remove it.

**Crash recovery correctness**, not timed. `KillNineRecoveryTest` SIGKILLs a child JVM while it
publishes at full speed, and requires the recovered state to be byte-identical to a fresh engine fed
the same commands. Example runs printed by the test: 16.3M commands journaled, snapshot at 14.5M,
1.76M replayed; and 7.5M journaled, snapshot at 7.37M, 90k replayed. All passed.

## 5. Coordinated omission: corrected vs naive

Every load-generator run records two histograms:
- **corrected:** from the *intended* send time, as above;
- **naive:** from the *actual* send time, which is what a closed-loop or naive open-loop tool reports.

p99.9, median of 3 runs:

| config | corrected p99.9 | naive p99.9 |
|---|---|---|
| fsync-EVERY_BATCH | 5411 [219–6140] | 4964 [216–5538] |
| fsync-NONE | 8110 [4342–8237] | 7487 [2679–8237] |
| gc-Epsilon-pooling-true | 8897 [7180–9839] | 7991 [6169–8561] |
| gc-G1-pooling-false | 31752 [8790–66454] | 13320 [6726–16613] |
| gc-G1-pooling-true | 8327 [7602–9421] | 7602 [7348–8413] |
| gc-Z-pooling-false | 10945 [9093–11747] | 10199 [7885–10625] |
| gc-Z-pooling-true | 11682 [9118–16048] | 8634 [8167–12304] |
| pretouch-false | 2044 [1393–5931] | 1475 [971–4481] |
| pretouch-true | 6738 [3756–7578] | 5915 [2116–6615] |
| rate-100000 | 5652 [3762–7524] | 4309 [2456–7184] |
| rate-1000000 | 6988 [6463–7107] | 5550 [5226–7102] |
| rate-2000000 | 8552 [5186–12419] | 6488 [3918–10674] |
| rate-250000 | 5218 [4465–5878] | 4678 [2902–4854] |
| rate-500000 | 8159 [6111–9658] | 7393 [5612–8372] |
| wait-BLOCKING | 3946 [122–4735] | 2107 [121–3279] |
| wait-BUSY_SPIN | 6066 [3791–6169] | 4252 [2511–4559] |
| wait-SLEEPING | 1820 [836–1936] | 1055 [502–1150] |
| wait-YIELDING | 10928 [5308–14778] | 10928 [4391–14778] |

The naive number is lower or equal in every configuration. In the worst case shown (G1, pooling off) it hides
more than half of the p99.9: 13.3 ms instead of 31.8 ms. In the closed-loop JMH benchmarks the effect
cannot appear at all, which is why JMH is used only for mean cost and allocation, and the open-loop
generator for latency.

## 6. End-to-end load test (Gatling through the gateway)

Gatling 3.16 (Java DSL), `loadtest/.../OrderEntrySimulation`.

**The load.** An open injection model: virtual users *arrive* at a fixed rate whether or not earlier
requests have finished, so a slow system is measured as slow. Each session submits a passive limit
order, sends an aggressive IOC in 30% of sessions, and cancels the passive order if it is still
resting. That is about 2.1 requests per session, from three authenticated clients across four symbols.

**The path.** HTTP, then API-key auth, then one Redis Lua call (idempotency, token bucket and id
assignment), then pre-trade risk, then gRPC to the engine (journal and matching), then the response
and a Redis write of the stored outcome. Kafka, post-trade and market data run concurrently.

**Where it ran.** The whole Compose stack ran in a Colima Linux VM with **4 vCPUs and 6 GiB**:
Kafka, Postgres, Redis, Prometheus, Grafana, the OTel collector, Jaeger and four JVMs. For the 600
and 1 000 sessions/s rows, Gatling itself also ran inside that VM, on the Compose network (see below).
The engine uses `SLEEPING`/`BLOCKING` wait strategies here, not busy-spin. The rate limiter was set
high enough (`RATE_LIMIT_CAPACITY=2000`, `RATE_LIMIT_REFILL=1000`) not to interfere.

```bash
docker compose -f deploy/compose.yaml up -d --build
./mvnw -pl loadtest gatling:test -Dgateway=http://localhost:8080 -Drate=200 -Dduration=60   # from the host
scripts/loadtest-in-docker.sh 600 60                                                          # inside the network
scripts/loadtest-in-docker.sh 1000 60
```

| Sessions/s | Where Gatling ran | Requests | Throughput | Failed | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|---|
| 200 | host | 29 814 | 426 req/s | 0 | 1 ms | 2 ms | **4 ms** | 33 ms |
| 600 | Compose network | 89 512 | 1 279 req/s | 0 | 1 ms | 3 ms | **13 ms** | 98 ms |
| 1 000 | Compose network | 149 503 | 2 136 req/s | 0 | 1 ms | 11 ms | **42 ms** | 211 ms |

Raw output: [`gatling-200.txt`](results/gatling-200.txt), [`gatling-600-in-network.txt`](results/gatling-600-in-network.txt),
[`gatling-1000-in-network.txt`](results/gatling-1000-in-network.txt).

**Consistency after these runs.** I queried Postgres directly:
- 5 786 trades, all distinct
- every symbol's net position across clients is exactly 0, with bought = sold per symbol
- total shares bought across clients (175 234) equal total shares traded
- the outbox is fully published, and the executions consumer has zero lag

### Problems found while load testing, and what they turned out to be

- **Colima's port forwarding fails above about 1 800 req/s from the host.** It's the VM's port
  forwarder, not the services. Every failed request was `Connection refused`, the Docker socket
  stopped answering on the Mac, and yet every container was healthy when inspected *inside* the VM.
  This is why the higher rates run Gatling inside the Compose network (`scripts/loadtest-in-docker.sh`).
- **Idle sleep inflated the tails.** A first round of runs showed p99 around 300 ms and max around
  1.5 s. Those runs overlapped the laptop's idle sleep: Gatling times requests with wall-clock time,
  which keeps moving while the machine sleeps. With sleep prevented (`caffeinate`), the same profile
  gives p99 4 ms. I first blamed cold JVMs, which was wrong.
- **A test bug hid the matching path.** The first version of the simulation priced its "aggressive"
  orders on the wrong side, so they never traded. The engine's counters caught it: 72 508 accepted
  orders and only 1 trade. Fixed before the runs above.

### Post-trade drain rate: one transaction per record vs per batch

How fast post-trade catches up on a backlog of execution reports, inside the same Compose VM:
1. Stop post-trade.
2. Run 30 s of the 600 sessions/s Gatling profile.
3. Start post-trade, and time from partition assignment until the consumer lag reaches zero.

One-transaction-per-record is reproduced with `max.poll.records=1` on the same code. Single run per
mode: `scripts/measure-posttrade-drain.sh 500` and `scripts/measure-posttrade-drain.sh 1`. Raw:
[`posttrade-drain.txt`](results/posttrade-drain.txt).

| Mode | Backlog | Time to drain | Rate |
|---|---|---|---|
| One Postgres transaction per record | 61 137 executions | 27.4 s | **2 231 /s** |
| One transaction per Kafka poll (≤ 500 records) | 60 688 executions | 7.4 s | **8 201 /s** |

Every record still gets its `(shard, seq)` dedupe claim inside the transaction. Batching only
amortises the commit, which is a synchronous fsync on the VM's disk. An earlier, incorrect
measurement of 143/s came from sampling a consumer that was just finishing its backlog. It is not used
anywhere.
