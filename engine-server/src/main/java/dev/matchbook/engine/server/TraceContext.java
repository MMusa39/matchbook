package dev.matchbook.engine.server;

import dev.matchbook.engine.pipeline.OutboundEvent;

/**
 * W3C trace context ({@code traceparent: 00-<32 hex trace id>-<16 hex span id>-<2 hex flags>}) packed
 * into primitives so it can ride through the engine's rings without allocation.
 */
final class TraceContext {

    private TraceContext() {
    }

    /** Parsed form: trace id high and low 64 bits, span id, flags. {@code null} if absent or malformed. */
    record Parsed(long traceIdHigh, long traceIdLow, long spanId, byte flags) {
    }

    static Parsed parse(String traceparent) {
        if (traceparent == null || traceparent.length() != 55 || traceparent.charAt(2) != '-'
                || traceparent.charAt(35) != '-' || traceparent.charAt(52) != '-') {
            return null;
        }
        try {
            return new Parsed(
                    Long.parseUnsignedLong(traceparent, 3, 19, 16),
                    Long.parseUnsignedLong(traceparent, 19, 35, 16),
                    Long.parseUnsignedLong(traceparent, 36, 52, 16),
                    (byte) Integer.parseInt(traceparent, 53, 55, 16));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static String traceparent(OutboundEvent event) {
        if (event.traceIdHigh() == 0 && event.traceIdLow() == 0) {
            return null;
        }
        return "00-" + hex16(event.traceIdHigh()) + hex16(event.traceIdLow()) + "-" + hex16(event.spanId())
                + "-" + String.format("%02x", event.traceFlags() & 0xFF);
    }

    private static String hex16(long value) {
        String hex = Long.toHexString(value);
        return "0".repeat(16 - hex.length()) + hex;
    }
}
