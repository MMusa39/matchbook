package dev.matchbook.engine.pipeline;

/**
 * Journal durability.
 * <ul>
 *     <li>{@link #NONE}: records are in the OS page cache once written to the mapping. They survive a
 *     crash of the engine process (including {@code kill -9}), but not a kernel panic or power loss.</li>
 *     <li>{@link #EVERY_BATCH}: the journal stage forces the written range to disk at the end of every
 *     Disruptor batch, before the matching stage can see the batch (group commit). Survives power
 *     loss at the cost of a device flush per batch.</li>
 * </ul>
 */
public enum FsyncPolicy {
    NONE,
    EVERY_BATCH
}
