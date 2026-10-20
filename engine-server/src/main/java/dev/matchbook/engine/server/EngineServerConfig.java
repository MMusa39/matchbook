package dev.matchbook.engine.server;

import dev.matchbook.engine.pipeline.FsyncPolicy;
import dev.matchbook.engine.pipeline.WaitStrategyType;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Engine server settings, read from environment variables (12-factor style, so Docker Compose and Helm
 * configure it the same way). Every setting has a local default.
 */
public record EngineServerConfig(
        int grpcPort,
        int metricsPort,
        Path dataDirectory,
        List<SymbolDefinition> symbols,
        int maxOrdersPerShard,
        WaitStrategyType waitStrategy,
        WaitStrategyType outboundWaitStrategy,
        FsyncPolicy fsync,
        Duration snapshotInterval,
        String kafkaBootstrapServers,
        String tradesTopic,
        String executionsTopic) {

    public static EngineServerConfig fromEnvironment() {
        return fromMap(System.getenv());
    }

    public static EngineServerConfig fromMap(Map<String, String> env) {
        return new EngineServerConfig(
                Integer.parseInt(env.getOrDefault("ENGINE_GRPC_PORT", "9090")),
                Integer.parseInt(env.getOrDefault("ENGINE_METRICS_PORT", "9100")),
                Path.of(env.getOrDefault("ENGINE_DATA_DIR", "data/engine")),
                loadSymbols(env.get("MATCHBOOK_SYMBOLS_FILE")),
                Integer.parseInt(env.getOrDefault("ENGINE_MAX_ORDERS_PER_SHARD", "1000000")),
                WaitStrategyType.valueOf(env.getOrDefault("ENGINE_WAIT_STRATEGY", "YIELDING")),
                WaitStrategyType.valueOf(env.getOrDefault("ENGINE_OUTBOUND_WAIT_STRATEGY", "BLOCKING")),
                FsyncPolicy.valueOf(env.getOrDefault("ENGINE_FSYNC", "NONE")),
                Duration.parse(env.getOrDefault("ENGINE_SNAPSHOT_INTERVAL", "PT30S")),
                env.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", ""),
                env.getOrDefault("KAFKA_TRADES_TOPIC", "matchbook.trades"),
                env.getOrDefault("KAFKA_EXECUTIONS_TOPIC", "matchbook.executions"));
    }

    /** Loads symbol reference data from a file, or the bundled {@code symbols.json} when no file is given. */
    public static List<SymbolDefinition> loadSymbols(String file) {
        JsonMapper mapper = JsonMapper.builder().build();
        TypeReference<List<SymbolDefinition>> type = new TypeReference<>() {
        };
        try {
            if (file != null && !file.isBlank()) {
                return mapper.readValue(Files.readString(Path.of(file)), type);
            }
            try (InputStream in = EngineServerConfig.class.getResourceAsStream("/symbols.json")) {
                return mapper.readValue(in, type);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public boolean kafkaEnabled() {
        return !kafkaBootstrapServers.isBlank();
    }
}
