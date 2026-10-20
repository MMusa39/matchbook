package dev.matchbook.engine.pipeline;

/**
 * Counters written by the matching thread and read by others for monitoring. Fields are plain longs:
 * readers may see slightly stale values, which is fine for metrics and keeps the hot path free of
 * memory barriers. 64-bit reads and writes of {@code long} are not torn on 64-bit JVMs in practice
 * (JLS 17.7 permits tearing; HotSpot on 64-bit platforms does not tear aligned longs).
 */
public final class PipelineMetrics {

    long commandsMatched;
    long matchNanosTotal;
    long snapshots;
    long lastSnapshotPauseNanos;

    public long commandsMatched() {
        return commandsMatched;
    }

    public long matchNanosTotal() {
        return matchNanosTotal;
    }

    public long snapshots() {
        return snapshots;
    }

    public long lastSnapshotPauseNanos() {
        return lastSnapshotPauseNanos;
    }
}
