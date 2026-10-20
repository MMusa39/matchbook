package dev.matchbook.engine.core;

/** Order side. Codes are stable and used by the journal and snapshot formats. */
public enum Side {
    BUY(0),
    SELL(1);

    private static final Side[] BY_CODE = {BUY, SELL};

    private final int code;

    Side(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public Side opposite() {
        return this == BUY ? SELL : BUY;
    }

    /** Returns the side for a code, or {@code null} if the code is unknown. Allocation-free. */
    public static Side fromCode(int code) {
        return code >= 0 && code < BY_CODE.length ? BY_CODE[code] : null;
    }
}
