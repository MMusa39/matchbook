package dev.matchbook.gateway.admission;

/** Result of admitting a request through Redis (rate limit + idempotency + id assignment). */
public sealed interface Admission {

    /** New request: go ahead with this internal numeric client order id. */
    record Admitted(long internalClOrdId) implements Admission {
    }

    /** Over the client's rate limit. */
    record RateLimited(long retryAfterMillis) implements Admission {
    }

    /** Seen before and finished: return the stored outcome. */
    record Completed(String storedOutcome) implements Admission {
    }

    /** Seen before and still in flight, or its outcome is unknown (an engine timeout). */
    record InProgress(boolean outcomeUnknown) implements Admission {
    }
}
