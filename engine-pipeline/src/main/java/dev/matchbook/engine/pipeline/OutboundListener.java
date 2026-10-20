package dev.matchbook.engine.pipeline;

/**
 * Consumes engine output from the outbound ring on its own thread. Each listener sees every event in
 * sequence order, independently of the others, so a slow listener only delays itself (until the ring
 * fills, at which point it back-pressures the matching thread).
 */
public interface OutboundListener {

    /**
     * @param event      the event; valid only for the duration of the call
     * @param endOfBatch true when no further events are currently available, a good time to flush
     */
    void onEvent(OutboundEvent event, boolean endOfBatch) throws Exception;

    /** Called once on the listener's thread before the first event. */
    default void onStart() {
    }

    /** Called once on the listener's thread after the last event. */
    default void onShutdown() {
    }
}
