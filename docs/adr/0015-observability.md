# 0015: Micrometer, Prometheus, Grafana-as-code and OpenTelemetry

**Status:** accepted

## Context

An exchange has to be operated, not just run: order rate, rejects and why, latency distributions,
queue build-up, downstream lag, and the path of a single order across services.

## Decision

- **Metrics: Micrometer everywhere**, scraped by Prometheus.
  - The Spring services use Actuator's `/actuator/prometheus`.
  - The engine registers a `PrometheusMeterRegistry` directly and serves `/metrics` and `/health` from
    the JDK HTTP server, so there is no Spring in the engine process.
- **Engine metrics are recorded on listener threads, never on the matching thread.** The matching
  thread only stamps `nanoTime` into ring slots: command start, and match end on the command's last
  event. The `EngineMetrics` listener turns those stamps into:
  - `matchbook_engine_match_seconds`: histogram of time in the matching stage
  - `matchbook_engine_latency_seconds`: histogram, ingress to last event
  - `matchbook_engine_commands_total{result}`: order rate
  - `matchbook_engine_rejects_total{reason}`
  - `matchbook_engine_queue_depth{ring}`: ring backlog, read from Disruptor cursors
  - JVM and GC metrics
- **Other services:**
  - Gateway: request outcomes and latency, rejects by reason, and the Resilience4j circuit breaker state.
  - Post-trade: Kafka consumer lag (`kafka_consumer_fetch_manager_records_lag_max`, bound by Spring
    Kafka), events applied vs duplicates, and outbox backlog.
  - Market data: sessions and message rate.
- **Dashboards as code:** `deploy/grafana/generate_dashboard.py` generates
  `dashboards/matchbook.json`. Provisioning files load it and the Prometheus and Jaeger data sources.
  There is no hand-edited JSON.
- **Tracing (OpenTelemetry):**
  - **Gateway and post-trade** use the Spring Boot 4 OpenTelemetry starter: HTTP and gRPC server spans,
    and Kafka consumer spans (`spring.kafka.listener.observation-enabled`), exported over OTLP.
  - **The rings break context.** No agent can follow a trace through the engine's rings, so the
    gateway puts its W3C `traceparent` *inside the engine command*.
  - **The engine span:** the engine carries it (as four primitives) through both rings and creates
    an `engine.process` span with the OTel SDK on a listener thread. The span is timed from the
    command's ingress to its last event.
  - **Into Kafka:** that span's context goes into the Kafka headers, so post-trade's consumer span
    joins the same trace.
  - **Export path:** collector → Jaeger.

## Consequences

- One trace shows gateway HTTP/gRPC → engine processing → post-trade consumption.
- Histograms have explicit minimum and maximum expected values (500 ns to 100 ms for the engine) so
  the Prometheus bucket counts stay bounded.
- Percentiles in Grafana come from `histogram_quantile` over buckets: they are estimates within bucket
  resolution. The benchmark numbers in RESULTS.md come from HdrHistogram and JMH, not from Prometheus.
