# 0009: Configurable wait strategies; CPU pinning on Linux only

**Status:** accepted

## Context

How a consumer thread waits for the next event decides both its latency and its CPU cost. The right
choice depends on the deployment: a dedicated, isolated server, or a shared laptop or Kubernetes
node.

## Decision

- **Wait strategy:** configurable per ring (`WaitStrategyType`): `BUSY_SPIN`, `YIELDING`, `SLEEPING`,
  `BLOCKING`. The trade-offs are documented on the enum and measured in RESULTS §3B. Defaults:
  `YIELDING` for local and Kubernetes runs; `BUSY_SPIN` for benchmarks.
- **CPU pinning:** configurable per stage (`journalCpu`, `matchingCpu`), implemented with OpenHFT
  Java-Thread-Affinity. On non-Linux systems the request is logged and ignored. macOS has no API to
  pin a thread to a core, and Apple Silicon mixes performance and efficiency cores that the scheduler
  moves threads between.

## Consequences

- **Laptop results are noisy.** On the development laptop (Apple M5: 4 performance + 6 efficiency
  cores, macOS) the scheduler occasionally deschedules a spinning thread for several milliseconds.
  This shows up as the load generator itself falling behind its schedule by as much as the maximum
  latency, and it dominates p99.9 and above. These numbers are reported as they are, not filtered
  out (RESULTS §3).
- **Verified on Linux** (RESULTS §8). In a Linux container, the kernel's `Cpus_allowed_list`
  confirms the journal and matching threads are pinned. Pinning them cut p99.9 by about 2.6× at
  100k commands/s, measured in a VM, so this is vCPU pinning.
- **Production setup:** pinning matters only together with `isolcpus`/`nohz_full` kernel isolation,
  IRQ affinity, and turning off frequency scaling. None of that is possible on the laptop or in a desktop
  Docker VM, so this project claims no bare-metal pinned-core numbers.
