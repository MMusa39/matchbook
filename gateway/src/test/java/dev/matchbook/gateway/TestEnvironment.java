package dev.matchbook.gateway;

import dev.matchbook.engine.server.EngineServer;
import dev.matchbook.engine.server.EngineServerConfig;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

/**
 * Shared infrastructure for gateway integration tests: a real Redis (Testcontainers) and a real matching
 * engine server, started once per test JVM.
 */
public final class TestEnvironment {

    public static final String KEY_1 = "local-dev-key-1";
    public static final String KEY_2 = "local-dev-key-2";

    @SuppressWarnings("resource")
    public static final GenericContainer<?> REDIS = new GenericContainer<>("redis:8.8.3-alpine").withExposedPorts(6379);
    private static EngineServer engine;

    private TestEnvironment() {
    }

    public static synchronized void start() {
        if (!REDIS.isRunning()) {
            REDIS.start();
        }
        if (engine == null) {
            try {
                Map<String, String> env = new HashMap<>();
                env.put("ENGINE_GRPC_PORT", "0");
                env.put("ENGINE_METRICS_PORT", "0");
                env.put("ENGINE_DATA_DIR", Files.createTempDirectory("gateway-it-engine").toString());
                env.put("ENGINE_MAX_ORDERS_PER_SHARD", "100000");
                env.put("ENGINE_SNAPSHOT_INTERVAL", "PT0S");
                engine = new EngineServer(EngineServerConfig.fromMap(env)).start();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static void register(DynamicPropertyRegistry registry) {
        start();
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("matchbook.engine-target", () -> "localhost:" + engine.port());
        registry.add("spring.grpc.server.port", () -> "0");
    }
}
