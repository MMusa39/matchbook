package dev.matchbook.engine.pipeline;

import dev.matchbook.engine.core.Command;

/**
 * One inbound ring-buffer slot. Pre-allocated by the Disruptor and overwritten by producers.
 *
 * <p>Besides the engine {@link Command} it carries data that is not part of the matching input and is
 * therefore not journaled: who to answer ({@code correlationId}), when the command entered the
 * process ({@code ingressNanos}, for latency) and the caller's trace context, so tracing survives the
 * asynchronous hop through the rings.
 */
public final class InboundSlot {

    public enum Kind {
        /** A trading command: journaled, then matched. */
        COMMAND,
        /** Take a snapshot at this exact point in the stream. Not journaled. */
        SNAPSHOT,
        /** Emit a full book image for {@code command.symbolId()} on the outbound ring. Not journaled. */
        BOOK_IMAGE
    }

    final Command command = new Command();
    Kind kind = Kind.COMMAND;
    long journalSeq;
    long correlationId;
    long ingressNanos;
    long traceIdHigh;
    long traceIdLow;
    long spanId;
    byte traceFlags;
    SnapshotRequest snapshotRequest;

    public Command command() {
        return command;
    }

    public InboundSlot kind(Kind kind) {
        this.kind = kind;
        return this;
    }

    public InboundSlot correlationId(long correlationId) {
        this.correlationId = correlationId;
        return this;
    }

    public InboundSlot ingressNanos(long ingressNanos) {
        this.ingressNanos = ingressNanos;
        return this;
    }

    public InboundSlot trace(long traceIdHigh, long traceIdLow, long spanId, byte flags) {
        this.traceIdHigh = traceIdHigh;
        this.traceIdLow = traceIdLow;
        this.spanId = spanId;
        this.traceFlags = flags;
        return this;
    }

    void clearMetadata() {
        kind = Kind.COMMAND;
        journalSeq = 0;
        correlationId = 0;
        ingressNanos = 0;
        traceIdHigh = 0;
        traceIdLow = 0;
        spanId = 0;
        traceFlags = 0;
        snapshotRequest = null;
    }
}
