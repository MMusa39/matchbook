package dev.matchbook.engine.core;

/**
 * Time in force.
 * <ul>
 *     <li>{@link #GTC}: rests until filled or cancelled (for the life of the session).</li>
 *     <li>{@link #IOC}: fills what it can immediately, the remainder is cancelled.</li>
 *     <li>{@link #FOK}: fills completely and immediately, or is cancelled with no fills.</li>
 * </ul>
 */
public enum TimeInForce {
    GTC(0),
    IOC(1),
    FOK(2);

    private static final TimeInForce[] BY_CODE = {GTC, IOC, FOK};

    private final int code;

    TimeInForce(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Returns the time in force for a code, or {@code null} if the code is unknown. Allocation-free. */
    public static TimeInForce fromCode(int code) {
        return code >= 0 && code < BY_CODE.length ? BY_CODE[code] : null;
    }
}
