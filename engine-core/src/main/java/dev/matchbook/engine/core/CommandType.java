package dev.matchbook.engine.core;

/** Kind of command the engine accepts. Codes are stable and used by the journal format. */
public enum CommandType {
    NEW_ORDER(0),
    CANCEL(1),
    REPLACE(2);

    private static final CommandType[] BY_CODE = {NEW_ORDER, CANCEL, REPLACE};

    private final int code;

    CommandType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Returns the command type for a code, or {@code null} if the code is unknown. Allocation-free. */
    public static CommandType fromCode(int code) {
        return code >= 0 && code < BY_CODE.length ? BY_CODE[code] : null;
    }
}
