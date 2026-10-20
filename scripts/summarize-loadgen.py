#!/usr/bin/env python3
"""Summarises open-loop load generator runs (docs/results/loadgen/*-rN.txt) as Markdown tables.

For each configuration it reports the median of each percentile across repetitions, and the range
(min-max) in brackets. Usage: scripts/summarize-loadgen.py [dir]
"""
import pathlib
import re
import statistics
import sys
from collections import defaultdict

directory = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "docs/results/loadgen")
runs = defaultdict(list)
for f in sorted(directory.glob("*-r[0-9].txt")):
    text = f.read_text()
    label = re.sub(r"-r\d+$", "", f.stem)
    co = re.search(r"CO-corrected\) us: (.*)", text).group(1)
    naive = re.search(r"naive\)\s+us: (.*)", text).group(1)
    values = {k: float(v) for k, v in re.findall(r"(p50|p90|p99|p99\.9|p99\.99|max)=([\d.]+)", co)}
    values["naive_p99.9"] = float(re.search(r"p99\.9=([\d.]+)", naive).group(1))
    lag = re.search(r"producer max lag behind schedule: ([\d.]+) us", text)
    gc = re.search(r"gc during measurement: (\d+) collections, (\d+) ms", text)
    cpu = re.search(r"process cpu: ([\d.]+) cores", text)
    values["lag_ms"] = float(lag.group(1)) / 1000
    values["gc_count"] = float(gc.group(1))
    values["gc_ms"] = float(gc.group(2))
    values["cpu"] = float(cpu.group(1))
    achieved = re.search(r"achieved=(\d+)/s", text)
    values["achieved"] = float(achieved.group(1))
    runs[label].append(values)


def cell(label, key, digits=2):
    xs = [r[key] for r in runs[label]]
    med = statistics.median(xs)
    fmt = f"{{:.{digits}f}}"
    if len(xs) == 1:
        return fmt.format(med)
    return f"{fmt.format(med)} [{fmt.format(min(xs))}–{fmt.format(max(xs))}]"


def table(title, labels, columns):
    print(f"\n#### {title}\n")
    print("| config | " + " | ".join(c[0] for c in columns) + " |")
    print("|---|" + "---|" * len(columns))
    for label in labels:
        if label not in runs:
            continue
        print(f"| {label} | " + " | ".join(cell(label, key, d) for _, key, d in columns) + " |")


latency = [("p50 µs", "p50", 2), ("p90 µs", "p90", 2), ("p99 µs", "p99", 1), ("p99.9 µs", "p99.9", 0),
           ("p99.99 µs", "p99.99", 0), ("max µs", "max", 0)]
table("A. Rate sweep", [f"rate-{r}" for r in (100000, 250000, 500000, 1000000, 2000000)],
      latency + [("producer lag ms", "lag_ms", 1)])
table("B. Wait strategies at 200k/s", [f"wait-{w}" for w in ("BUSY_SPIN", "YIELDING", "SLEEPING", "BLOCKING")],
      latency + [("CPU cores", "cpu", 2)])
table("C. Pooling × GC at 1M/s", ["gc-G1-pooling-true", "gc-G1-pooling-false", "gc-Z-pooling-true",
                                   "gc-Z-pooling-false", "gc-Epsilon-pooling-true"],
      latency + [("GCs", "gc_count", 0), ("GC ms", "gc_ms", 0)])
table("D. Journal pretouch at 200k/s", ["pretouch-true", "pretouch-false"], latency)
table("E. Durability at 100k/s", ["fsync-NONE", "fsync-EVERY_BATCH"], latency + [("CPU cores", "cpu", 2)])
table("Coordinated omission: corrected vs naive p99.9 (µs)", sorted(runs),
      [("corrected p99.9", "p99.9", 0), ("naive p99.9", "naive_p99.9", 0)])
