package dev.matchbook.gateway.api;

import dev.matchbook.gateway.config.GatewayProperties;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Collectors;

/**
 * Maps an API key to a client id. Configuration holds only SHA-256 digests of the keys, so a leaked
 * config file does not leak credentials. Lookup is by digest, so there is no per-character comparison
 * that could leak timing.
 */
@Component
public class ApiKeyAuthenticator {

    public static final String HEADER = "x-api-key";

    private final Map<String, Long> clientsByDigest;

    public ApiKeyAuthenticator(GatewayProperties properties) {
        this.clientsByDigest = properties.apiKeys().entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(e -> e.getValue().toLowerCase(), Map.Entry::getKey));
    }

    public OptionalLong authenticate(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) {
            return OptionalLong.empty();
        }
        Long client = clientsByDigest.get(sha256(apiKey));
        return client == null ? OptionalLong.empty() : OptionalLong.of(client);
    }

    public static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
