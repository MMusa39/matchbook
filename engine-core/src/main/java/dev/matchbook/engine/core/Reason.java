package dev.matchbook.engine.core;

/** Why an order was rejected or cancelled. {@link #NONE} for every other execution report. */
public enum Reason {
    NONE(0),
    // Validation rejects
    UNKNOWN_SYMBOL(1),
    INVALID_FIELD(2),
    INVALID_QUANTITY(3),
    INVALID_LOT_SIZE(4),
    INVALID_PRICE(5),
    INVALID_TICK_SIZE(6),
    PRICE_OUT_OF_BAND(7),
    INVALID_TIME_IN_FORCE(8),
    UNKNOWN_ORDER(9),
    ORDER_CAPACITY_EXCEEDED(10),
    // Cancel reasons
    CLIENT_CANCEL(11),
    IOC_REMAINDER(12),
    FOK_NOT_FILLED(13),
    SELF_TRADE_PREVENTION(14);

    private static final Reason[] BY_CODE = values();

    private final int code;

    Reason(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Returns the reason for a code, or {@code null} if the code is unknown. Allocation-free. */
    public static Reason fromCode(int code) {
        return code >= 0 && code < BY_CODE.length ? BY_CODE[code] : null;
    }
}
