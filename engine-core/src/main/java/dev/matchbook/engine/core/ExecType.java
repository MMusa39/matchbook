package dev.matchbook.engine.core;

/** What an execution report reports, loosely following FIX ExecType. */
public enum ExecType {
    NEW(0),
    PARTIAL_FILL(1),
    FILL(2),
    CANCELED(3),
    REPLACED(4),
    REJECTED(5),
    CANCEL_REJECTED(6),
    REPLACE_REJECTED(7);

    private static final ExecType[] BY_CODE =
            {NEW, PARTIAL_FILL, FILL, CANCELED, REPLACED, REJECTED, CANCEL_REJECTED, REPLACE_REJECTED};

    private final int code;

    ExecType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Returns the exec type for a code, or {@code null} if the code is unknown. Allocation-free. */
    public static ExecType fromCode(int code) {
        return code >= 0 && code < BY_CODE.length ? BY_CODE[code] : null;
    }
}
