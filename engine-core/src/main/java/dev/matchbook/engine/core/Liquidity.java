package dev.matchbook.engine.core;

/** Liquidity indicator on a fill: the resting order made liquidity, the incoming order took it. */
public enum Liquidity {
    NONE(0),
    MAKER(1),
    TAKER(2);

    private static final Liquidity[] BY_CODE = {NONE, MAKER, TAKER};

    private final int code;

    Liquidity(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Returns the indicator for a code, or {@code null} if the code is unknown. Allocation-free. */
    public static Liquidity fromCode(int code) {
        return code >= 0 && code < BY_CODE.length ? BY_CODE[code] : null;
    }
}
