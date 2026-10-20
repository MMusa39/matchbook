package dev.matchbook.engine.core;

/**
 * Self-trade prevention instruction, carried on the incoming (aggressing) order.
 * Two orders self-match when they belong to the same client.
 */
public enum StpMode {
    /** Cancel the resting order and keep matching the aggressor against the rest of the book. */
    CANCEL_RESTING(0),
    /** Cancel the remaining quantity of the aggressor; the resting order is untouched. */
    CANCEL_AGGRESSOR(1),
    /** Cancel both the resting order and the remaining quantity of the aggressor. */
    CANCEL_BOTH(2);

    private static final StpMode[] BY_CODE = {CANCEL_RESTING, CANCEL_AGGRESSOR, CANCEL_BOTH};

    private final int code;

    StpMode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Returns the mode for a code, or {@code null} if the code is unknown. Allocation-free. */
    public static StpMode fromCode(int code) {
        return code >= 0 && code < BY_CODE.length ? BY_CODE[code] : null;
    }
}
