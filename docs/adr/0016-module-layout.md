# 0016: Maven module layout

**Status:** accepted

| Module | Depends on | Why it is separate |
|---|---|---|
| `proto` | protobuf, grpc-stub | One schema compiled once and shared by every service |
| `engine-core` | Agrona only | Matching logic, enforced framework-free and deterministic by ArchUnit |
| `engine-pipeline` | engine-core, Disruptor, Agrona, Affinity | Journal, snapshots, recovery. No networking (ArchUnit), so it can be benchmarked and crash-tested in isolation |
| `engine-bench` | engine-pipeline, JMH, HdrHistogram | Benchmarks never ship with the server |
| `engine-server` | engine-pipeline, proto, grpc-netty, kafka-clients, Micrometer | The engine process. Plain Java: no Spring in the process that runs the matching thread |
| `gateway` | proto, Spring Boot | Order entry, auth, Redis, risk |
| `marketdata` | proto, Spring Boot | Public feed |
| `posttrade` | proto, Spring Boot, Kafka, JDBC, Flyway | Positions, P&L, outbox |
| `loadtest` | Gatling | Load tests against the running system |
| `deploy/` | (not a module) | Docker Compose, Helm chart, Grafana/Prometheus/OTel configuration |

The brief suggested putting the pipeline inside `engine-server`. It was split out because the open-loop
benchmark and the `kill -9` recovery test need the real pipeline and journal without gRPC, Kafka or
Netty on the classpath.

The parent POM imports the Spring Boot BOM for dependency versions only. Engine modules inherit
versions (JUnit, AssertJ, gRPC, Kafka), not Spring itself. Every other version is pinned in the parent.
