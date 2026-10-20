# Deployment

| Path | What |
|---|---|
| `compose.yaml` | The whole system plus Kafka, Redis, PostgreSQL, Prometheus, Grafana, OTel Collector and Jaeger |
| `docker/Dockerfile` | One build stage for every module; a small runtime stage per service (`--target engine|gateway|marketdata|posttrade`) |
| `config/symbols.json` | Symbol reference data shared by every service (the same file is bundled as each service's default) |
| `prometheus/prometheus.yml` | Scrape configuration |
| `grafana/generate_dashboard.py` | Generates `grafana/dashboards/matchbook.json`; edit the script, not the JSON |
| `grafana/provisioning/` | Data sources (Prometheus, Jaeger) and dashboard provider |
| `otel/collector.yaml` | OTLP in, Jaeger out |
| `helm/matchbook/` | Helm chart: engine StatefulSet with a persistent volume, Deployments for the Spring services, optional in-chart Kafka/Redis/Postgres |
| `kind/kind-config.yaml` | Local Kubernetes cluster used by `scripts/kind-up.sh` |

## Docker Compose

```bash
docker compose -f deploy/compose.yaml up -d --build     # first build takes a few minutes
docker compose -f deploy/compose.yaml ps
docker compose -f deploy/compose.yaml down              # add -v to drop the engine journal and Postgres data
```

The first build downloads every Maven dependency inside the build container; later builds reuse a
BuildKit cache. Docker Desktop needs about 5–6 GB of memory for the full stack.

The engine runs with `SLEEPING` (inbound) and `BLOCKING` (outbound) wait strategies here. Busy-spinning
threads would take most of Docker Desktop's CPUs and starve Kafka and the databases. Latency
benchmarks run outside Docker with busy-spin (see `docs/RESULTS.md`).

## Kubernetes (kind)

```bash
scripts/kind-up.sh
kubectl port-forward svc/matchbook-gateway 8080:8080
```

The engine is a single-replica StatefulSet. It is the single writer for its shards, and its journal
is not replicated (see `docs/DESIGN.md`). Its pod gets 60 seconds of termination grace, so a clean
shutdown can write a final snapshot. Secrets in `values.yaml` are local placeholders: override them
in any shared environment.
