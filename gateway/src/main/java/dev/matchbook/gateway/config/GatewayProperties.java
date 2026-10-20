package dev.matchbook.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

/**
 * Gateway settings ({@code matchbook.*}).
 *
 * @param engineTarget     gRPC target of the matching engine, for example {@code localhost:9090}
 * @param engineTimeout    deadline for each engine call
 * @param symbolsFile      symbol reference data; the bundled {@code symbols.json} when blank
 * @param apiKeys          client id to SHA-256 hex digest of that client's API key. Only digests are
 *                         configured, never keys.
 * @param rateLimit        per-client token bucket
 * @param idempotencyTtl   how long a client order id is remembered
 * @param risk             pre-trade risk limits, applied to every client
 * @param fix              FIX 4.4 acceptor
 */
@ConfigurationProperties("matchbook")
public record GatewayProperties(
        @DefaultValue("localhost:9090") String engineTarget,
        @DefaultValue("2s") Duration engineTimeout,
        @DefaultValue("") String symbolsFile,
        Map<Long, String> apiKeys,
        @DefaultValue RateLimit rateLimit,
        @DefaultValue("24h") Duration idempotencyTtl,
        @DefaultValue Risk risk,
        @DefaultValue Fix fix) {

    public GatewayProperties {
        apiKeys = apiKeys == null ? Map.of() : Map.copyOf(apiKeys);
    }

    /**
     * FIX 4.4 order entry. Clients log on with any SenderCompID, {@code TargetCompID = senderCompId}, and
     * their API key in {@code Password (554)}.
     */
    public record Fix(@DefaultValue("true") boolean enabled, @DefaultValue("9876") int port,
                      @DefaultValue("MATCHBOOK") String senderCompId) {
    }

    /** {@code capacity} tokens, refilled at {@code refillPerSecond}; each request costs one token. */
    public record RateLimit(@DefaultValue("100") int capacity, @DefaultValue("50") double refillPerSecond) {
    }

    /**
     * @param maxOrderQuantity largest single order, in shares
     * @param maxNotional      largest price × quantity of a single order, in currency units
     * @param maxPosition      largest absolute net position per client and symbol, counting open orders
     *                         as if they will fill
     * @param fatFingerPercent largest distance of a limit price from the last trade, in percent
     */
    public record Risk(
            @DefaultValue("100000") long maxOrderQuantity,
            @DefaultValue("5000000") BigDecimal maxNotional,
            @DefaultValue("500000") long maxPosition,
            @DefaultValue("10") BigDecimal fatFingerPercent) {
    }
}
