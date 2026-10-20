#!/usr/bin/env python3
"""Generates deploy/grafana/dashboards/matchbook.json. Edit this file, not the JSON.

    python3 deploy/grafana/generate_dashboard.py
"""
import json
import pathlib

DS = {"type": "prometheus", "uid": "prometheus"}
panels = []
next_id = 1
y = 0


def row(title):
    global next_id, y
    panels.append({"type": "row", "title": title, "id": next_id, "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}})
    next_id += 1
    y += 1


def ts(title, targets, unit="short", x=0, w=8, h=8, description=""):
    global next_id
    panels.append({
        "type": "timeseries", "title": title, "id": next_id, "datasource": DS, "description": description,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": unit, "custom": {"lineWidth": 1, "fillOpacity": 10}}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
        "targets": [{"refId": chr(65 + i), "datasource": DS, "expr": e, "legendFormat": l}
                    for i, (e, l) in enumerate(targets)],
    })
    next_id += 1


def advance(h=8):
    global y
    y += h


def quantiles(metric, by=""):
    group = f"le{', ' + by if by else ''}"
    suffix = f" {{{{{by}}}}}" if by else ""
    return [(f"histogram_quantile({q}, sum by ({group}) (rate({metric}_bucket[1m])))", f"p{label}{suffix}")
            for q, label in [("0.5", "50"), ("0.99", "99"), ("0.999", "99.9")]]


row("Engine")
ts("Commands / s by response", [("sum by (result) (rate(matchbook_engine_commands_total[30s]))", "{{result}}")],
   "ops", 0, description="Order rate: every command, by its first response (NEW, REJECTED, CANCELED, ...)")
ts("Trades / s", [("sum by (shard) (rate(matchbook_engine_trades_total[30s]))", "shard {{shard}}")], "ops", 8)
ts("Rejects / s by reason", [
    ("sum by (reason) (rate(matchbook_engine_rejects_total[1m]))", "engine {{reason}}"),
    # Only rejects decided by the gateway itself; engine rejects passing through are counted on the engine line.
    ("sum by (reason) (rate(matchbook_gateway_rejects_total{reason=~\"RISK_.*|RATE_LIMITED.*|REDIS_.*|ENGINE_.*|OUTCOME_.*|DUPLICATE_.*\"}[1m]))",
     "gateway {{reason}}")], "ops", 16)
advance()
ts("Match time (matching stage)", quantiles("matchbook_engine_match_seconds"), "s", 0,
   description="Time inside the matching stage per command, measured on the matching thread")
ts("Engine latency (ingress to last event)", quantiles("matchbook_engine_latency_seconds"), "s", 8,
   description="From the gRPC thread publishing into the ring to the command's last event reaching a listener")
ts("Ring queue depth", [("max by (shard, ring) (matchbook_engine_queue_depth)", "shard {{shard}} {{ring}}")],
   "short", 16)
advance()

row("Gateway")
ts("Requests / s by outcome", [("sum by (kind, result) (rate(matchbook_gateway_requests_total[30s]))",
                                "{{kind}} {{result}}")], "ops", 0)
ts("Gateway request latency", quantiles("matchbook_gateway_request_seconds", "kind"), "s", 8)
ts("Engine circuit breaker", [("max by (state) (resilience4j_circuitbreaker_state{name=\"engine\"})", "{{state}}")],
   "short", 16, description="1 for the current state")
advance()

row("Post-trade")
# Kafka's client metrics tag topics twice (dots and underscores) and add a topic-less total: keep the real names.
ts("Kafka consumer lag", [("max by (topic) (kafka_consumer_fetch_manager_records_lag_max{application=\"posttrade\", topic=~\"matchbook\\\\..*\"})",
                           "{{topic}}")], "short", 0)
ts("Events applied vs duplicates", [("sum by (result) (rate(matchbook_posttrade_events_total[1m]))", "{{result}}")],
   "ops", 8, description="Duplicates are re-deliveries removed by the processed_events key")
ts("Outbox backlog", [("max(matchbook_posttrade_outbox_backlog)", "unpublished rows"),
                      ("sum(rate(matchbook_posttrade_outbox_published_total[1m]))", "published / s")], "short", 16)
advance()

row("Market data and JVM")
ts("WebSocket sessions and messages", [("max(matchbook_marketdata_sessions)", "sessions"),
                                       ("sum(rate(matchbook_marketdata_messages_total[30s]))", "messages / s")],
   "short", 0)
ts("GC pause time / s", [("sum by (application) (rate(jvm_gc_pause_seconds_sum[1m]))", "{{application}}")], "s", 8)
ts("Heap used", [("sum by (application) (jvm_memory_used_bytes{area=\"heap\"})", "{{application}}")], "bytes", 16)
advance()

dashboard = {
    "uid": "matchbook-overview", "title": "Matchbook overview", "tags": ["matchbook"], "timezone": "browser",
    "schemaVersion": 39, "version": 1, "refresh": "5s", "time": {"from": "now-15m", "to": "now"},
    "panels": panels, "templating": {"list": []}, "annotations": {"list": []},
}
out = pathlib.Path(__file__).parent / "dashboards" / "matchbook.json"
out.write_text(json.dumps(dashboard, indent=2) + "\n")
print(f"wrote {out} with {len(panels)} panels")
