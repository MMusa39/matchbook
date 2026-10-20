package dev.matchbook.engine.pipeline;

/**
 * Engine state as of a journal position: applying journal records after {@code journalSeq} to this
 * state reproduces the live engine.
 *
 * @param journalSeq  last journal record reflected in {@code state}
 * @param state       {@link dev.matchbook.engine.core.EngineSnapshot} encoding
 * @param pauseNanos  how long the matching thread spent serialising it
 */
public record Snapshot(long journalSeq, byte[] state, long pauseNanos) {
}
