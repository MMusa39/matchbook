#!/usr/bin/env bash
# measure-posttrade-drain.sh <max.poll.records> : build a backlog with posttrade stopped, then time how long it takes to drain it
set -euo pipefail
cd /Users/moham/Documents/Projects/matchbook
POLL=$1
lag(){ docker exec matchbook-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
        --describe --group executions 2>/dev/null | awk '/matchbook.executions/ {l+=$6} END {print l+0}'; }
docker compose -f deploy/compose.yaml stop posttrade >/dev/null 2>&1
docker rm -f pt-drain >/dev/null 2>&1 || true
scripts/loadtest-in-docker.sh 600 30 > ${TMPDIR:-/tmp}/drain-gatling-$POLL.log 2>&1 || true
BACKLOG=$(lag)
docker compose -f deploy/compose.yaml run -d --no-deps --name pt-drain -e SPRING_KAFKA_CONSUMER_MAX_POLL_RECORDS=$POLL posttrade >/dev/null 2>&1
until docker logs pt-drain 2>&1 | grep -q "executions: partitions assigned"; do sleep 0.2; done
T0=$(python3 -c 'import time;print(time.time())')
while true; do
  L=$(lag)
  NOW=$(python3 -c 'import time;print(time.time())')
  if [ "$L" -eq 0 ]; then break; fi
  if python3 -c "import sys; sys.exit(0 if $NOW-$T0 > 120 else 1)"; then break; fi
  sleep 0.5
done
SECS=$(python3 -c "print(round($NOW-$T0,1))")
docker rm -f pt-drain >/dev/null
echo "max.poll.records=$POLL backlog=$BACKLOG remaining_lag=$L seconds=$SECS rate=$(python3 -c "print(int(($BACKLOG-$L)/max($SECS,0.1)))")/s"
