package dev.matchbook.gateway.admission;

import dev.matchbook.gateway.config.GatewayProperties;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * Per-request admission control in Redis. One Lua script ({@code scripts/admit.lua}) does idempotency,
 * the token-bucket rate limit and internal id assignment atomically, in a single round trip
 * (EVALSHA, with Spring falling back to EVAL if the script is not cached). See ADR-0011 and ADR-0012.
 *
 * <p>Redis being unreachable fails <em>closed</em>: the exception propagates and the order is rejected
 * as unavailable. Accepting orders without duplicate protection or rate limiting is not safe for a
 * venue.
 */
@Component
public class RedisAdmission {

    static final String PENDING = "PENDING:";
    static final String DONE = "DONE:";
    static final String UNKNOWN = "UNKNOWN:";
    private static final Duration ID_MAP_TTL = Duration.ofDays(7);

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> ADMIT = RedisScript.of(new ClassPathResource("scripts/admit.lua"), List.class);

    private final StringRedisTemplate redis;
    private final GatewayProperties properties;

    public RedisAdmission(StringRedisTemplate redis, GatewayProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    public Admission admit(long clientId, String clientOrderId) {
        String prefix = "mb:{" + clientId + "}:";
        List<?> result = redis.execute(ADMIT,
                List.of(prefix + "rl", idempotencyKey(clientId, clientOrderId), prefix + "seq", prefix + "clord"),
                Integer.toString(properties.rateLimit().capacity()),
                Double.toString(properties.rateLimit().refillPerSecond()),
                "1",
                Long.toString(properties.idempotencyTtl().toMillis()),
                clientOrderId,
                Long.toString(ID_MAP_TTL.toMillis()));
        String status = String.valueOf(result.get(0));
        String value = String.valueOf(result.get(1));
        return switch (status) {
            case "ADMITTED" -> new Admission.Admitted(Long.parseLong(value));
            case "RATE_LIMITED" -> new Admission.RateLimited(Long.parseLong(value));
            case "DUPLICATE" -> {
                if (value.startsWith(DONE)) {
                    yield new Admission.Completed(value.substring(DONE.length()));
                }
                yield new Admission.InProgress(value.startsWith(UNKNOWN));
            }
            default -> throw new IllegalStateException("unexpected admission result " + status);
        };
    }

    /** Stores the final outcome so a retry with the same client order id gets the same answer. */
    public void complete(long clientId, String clientOrderId, String outcomeJson) {
        redis.opsForValue().set(idempotencyKey(clientId, clientOrderId), DONE + outcomeJson, properties.idempotencyTtl());
    }

    /**
     * The engine call timed out: the order may or may not exist. The key is kept so a blind retry cannot
     * create a second order; the client learns the real outcome from its execution stream.
     */
    public void markUnknown(long clientId, String clientOrderId) {
        redis.opsForValue().set(idempotencyKey(clientId, clientOrderId), UNKNOWN, properties.idempotencyTtl());
    }

    /** The request definitely never reached the engine (circuit open, ring full): allow a retry. */
    public void release(long clientId, String clientOrderId) {
        redis.delete(idempotencyKey(clientId, clientOrderId));
    }

    /** External client order id for an internal one, or {@code null} if unknown or expired. */
    public String externalClOrdId(long clientId, long internalClOrdId) {
        Object value = redis.opsForHash().get("mb:{" + clientId + "}:clord", Long.toString(internalClOrdId));
        return value == null ? null : value.toString();
    }

    private static String idempotencyKey(long clientId, String clientOrderId) {
        return "mb:{" + clientId + "}:idem:" + clientOrderId;
    }
}
