package dev.matchbook.gateway.engine;

/**
 * The engine could not be asked. {@code definitelyNotSent} distinguishes "never left the gateway"
 * (circuit open, engine busy: safe to retry) from "may have been processed" (deadline exceeded).
 */
public class EngineUnavailableException extends RuntimeException {

    private final boolean definitelyNotSent;

    public EngineUnavailableException(String message, boolean definitelyNotSent, Throwable cause) {
        super(message, cause);
        this.definitelyNotSent = definitelyNotSent;
    }

    public boolean definitelyNotSent() {
        return definitelyNotSent;
    }
}
