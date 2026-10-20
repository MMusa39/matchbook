package dev.matchbook.engine.core;

/** Order type. Codes are stable and used by the journal and snapshot formats. */
public enum OrderType {
    LIMIT(0),
    MARKET(1);

    private static final OrderType[] BY_CODE = {LIMIT, MARKET};

    private final int code;

    OrderType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Returns the type for a code, or {@code null} if the code is unknown. Allocation-free. */
    public static OrderType fromCode(int code) {
        return code >= 0 && code < BY_CODE.length ? BY_CODE[code] : null;
    }
}
