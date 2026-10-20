package dev.matchbook.engine.pipeline;

import dev.matchbook.engine.core.EngineConfig;

import java.nio.file.Path;
import java.time.Duration;

/**
 * Configuration of one engine shard's pipeline.
 *
 * @param engine               matching engine config (symbols, capacity, shard id)
 * @param dataDirectory        where journal segments and snapshots live; one directory per shard
 * @param inboundRingSize      inbound ring slots, a power of two
 * @param outboundRingSize     outbound ring slots, a power of two; one command can emit many events
 * @param inboundWait          wait strategy of the journal and matching stages
 * @param outboundWait         wait strategy of the outbound listeners
 * @param multiProducer        {@code true} if several threads publish (gRPC threads, the snapshot timer).
 *                             With {@code false}, every call that publishes ({@code publish},
 *                             {@code snapshot}, {@code requestBookImage}, {@code close}) must come from
 *                             one thread, and scheduled snapshots are not allowed.
 * @param fsync                journal durability, see {@link FsyncPolicy}
 * @param journalSegmentBytes  size of each journal file; a new one is started when full
 * @param snapshotInterval     how often to snapshot automatically, or {@code Duration.ZERO} for never
 * @param snapshotsToKeep      older snapshots beyond this many are deleted
 * @param journalCpu           CPU to pin the journal thread to, or -1 (Linux only, see ADR-0009)
 * @param matchingCpu          CPU to pin the matching thread to, or -1
 * @param pretouchJournal      pre-fault journal pages when a segment is mapped (see {@link Journal#pretouch})
 */
public record PipelineConfig(
        EngineConfig engine,
        Path dataDirectory,
        int inboundRingSize,
        int outboundRingSize,
        WaitStrategyType inboundWait,
        WaitStrategyType outboundWait,
        boolean multiProducer,
        FsyncPolicy fsync,
        long journalSegmentBytes,
        Duration snapshotInterval,
        int snapshotsToKeep,
        int journalCpu,
        int matchingCpu,
        boolean pretouchJournal) {

    public PipelineConfig {
        if (Integer.bitCount(inboundRingSize) != 1 || Integer.bitCount(outboundRingSize) != 1) {
            throw new IllegalArgumentException("ring sizes must be powers of two");
        }
        if (journalSegmentBytes < 64 * 1024) {
            throw new IllegalArgumentException("journalSegmentBytes must be at least 64 KiB");
        }
        if (snapshotsToKeep < 1) {
            throw new IllegalArgumentException("snapshotsToKeep must be >= 1");
        }
        if (!multiProducer && snapshotInterval.isPositive()) {
            // The snapshot timer publishes control slots from its own thread: a second producer.
            throw new IllegalArgumentException("scheduled snapshots need a multi-producer inbound ring");
        }
    }

    /** Defaults suitable for tests and local runs: yielding waits, no fsync, no pinning. */
    public static PipelineConfig defaults(EngineConfig engine, Path dataDirectory) {
        return new PipelineConfig(engine, dataDirectory, 1 << 14, 1 << 16, WaitStrategyType.YIELDING,
                WaitStrategyType.YIELDING, true, FsyncPolicy.NONE, 64L * 1024 * 1024, Duration.ZERO, 3, -1, -1, false);
    }

    public PipelineConfig withWaitStrategies(WaitStrategyType inbound, WaitStrategyType outbound) {
        return new PipelineConfig(engine, dataDirectory, inboundRingSize, outboundRingSize, inbound, outbound,
                multiProducer, fsync, journalSegmentBytes, snapshotInterval, snapshotsToKeep, journalCpu, matchingCpu, pretouchJournal);
    }

    public PipelineConfig withFsync(FsyncPolicy policy) {
        return new PipelineConfig(engine, dataDirectory, inboundRingSize, outboundRingSize, inboundWait, outboundWait,
                multiProducer, policy, journalSegmentBytes, snapshotInterval, snapshotsToKeep, journalCpu, matchingCpu, pretouchJournal);
    }

    public PipelineConfig withSnapshotInterval(Duration interval) {
        return new PipelineConfig(engine, dataDirectory, inboundRingSize, outboundRingSize, inboundWait, outboundWait,
                multiProducer, fsync, journalSegmentBytes, interval, snapshotsToKeep, journalCpu, matchingCpu, pretouchJournal);
    }

    public PipelineConfig withJournalSegmentBytes(long bytes) {
        return new PipelineConfig(engine, dataDirectory, inboundRingSize, outboundRingSize, inboundWait, outboundWait,
                multiProducer, fsync, bytes, snapshotInterval, snapshotsToKeep, journalCpu, matchingCpu, pretouchJournal);
    }

    public PipelineConfig withProducer(boolean multi) {
        return new PipelineConfig(engine, dataDirectory, inboundRingSize, outboundRingSize, inboundWait, outboundWait,
                multi, fsync, journalSegmentBytes, snapshotInterval, snapshotsToKeep, journalCpu, matchingCpu, pretouchJournal);
    }

    public PipelineConfig withPretouchJournal(boolean enabled) {
        return new PipelineConfig(engine, dataDirectory, inboundRingSize, outboundRingSize, inboundWait, outboundWait,
                multiProducer, fsync, journalSegmentBytes, snapshotInterval, snapshotsToKeep, journalCpu, matchingCpu,
                enabled);
    }

    public PipelineConfig withCpus(int journal, int matching) {
        return new PipelineConfig(engine, dataDirectory, inboundRingSize, outboundRingSize, inboundWait, outboundWait,
                multiProducer, fsync, journalSegmentBytes, snapshotInterval, snapshotsToKeep, journal, matching, pretouchJournal);
    }
}
