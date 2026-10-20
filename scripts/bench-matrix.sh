#!/usr/bin/env bash
# Reproduces every engine latency number in docs/RESULTS.md.
# Usage: scripts/bench-matrix.sh [output-dir]   (default docs/results/loadgen)
# Build first: ./mvnw -q install -DskipTests
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${1:-docs/results/loadgen}
JAR=engine-bench/target/benchmarks.jar
HEAP="-Xms2g -Xmx2g -XX:+AlwaysPreTouch"
DUR=20
WARM=10
REPEAT=${REPEAT:-3}   # every configuration runs this many times; RESULTS.md reports median and range
mkdir -p "$OUT"

run() { # label jvm-flags... -- generator-args...
  local label=$1; shift
  local jvm=()
  while [[ $1 != "--" ]]; do jvm+=("$1"); shift; done; shift
  for r in $(seq 1 "$REPEAT"); do
    echo ">>> $label-r$r"
    java $HEAP "${jvm[@]}" -cp "$JAR" dev.matchbook.bench.OpenLoopLoadGenerator \
         --duration $DUR --warmup $WARM --label "$label-r$r" --out "$OUT" "$@"
  done
}

# A. Throughput sweep: where does latency break down?
for rate in 100000 250000 500000 1000000 2000000; do
  run "rate-$rate" -XX:+UseG1GC -- --rate $rate --wait BUSY_SPIN --outbound-wait YIELDING
done

# B. Wait strategies at a moderate rate.
for w in BUSY_SPIN YIELDING SLEEPING BLOCKING; do
  run "wait-$w" -XX:+UseG1GC -- --rate 200000 --wait $w --outbound-wait YIELDING
done

# C. Object pooling x garbage collector at a high rate.
for gc in G1 Z; do
  for pooling in true false; do
    run "gc-$gc-pooling-$pooling" -XX:+Use${gc}GC -- --rate 1000000 --wait BUSY_SPIN --outbound-wait YIELDING --pooling $pooling
  done
done
run "gc-Epsilon-pooling-true" -XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC -- \
    --rate 1000000 --wait BUSY_SPIN --outbound-wait YIELDING --pooling true

# D. Journal page pre-faulting.
for pt in true false; do
  run "pretouch-$pt" -XX:+UseG1GC -- --rate 200000 --wait BUSY_SPIN --outbound-wait YIELDING --pretouch $pt
done

# E. Durability: fsync per batch.
for fs in NONE EVERY_BATCH; do
  run "fsync-$fs" -XX:+UseG1GC -- --rate 100000 --wait BUSY_SPIN --outbound-wait YIELDING --fsync $fs
done

# F. Restart time.
java $HEAP -cp "$JAR" dev.matchbook.bench.RecoveryTimer --commands 10000000 | tee "$OUT/recovery-journal-only.txt"
java $HEAP -cp "$JAR" dev.matchbook.bench.RecoveryTimer --commands 10000000 --snapshot-at 9000000 | tee "$OUT/recovery-snapshot.txt"
