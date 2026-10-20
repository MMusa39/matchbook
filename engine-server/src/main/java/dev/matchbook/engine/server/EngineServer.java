package dev.matchbook.engine.server;

import dev.matchbook.engine.core.EngineConfig;
import dev.matchbook.engine.core.SymbolConfig;
import dev.matchbook.engine.pipeline.EnginePipeline;
import dev.matchbook.engine.pipeline.OutboundListener;
import dev.matchbook.engine.pipeline.PipelineConfig;
import dev.matchbook.proto.v1.EngineEvent;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The engine process: one {@link EnginePipeline} per shard, a gRPC server in front, and listeners that
 * answer requests, stream events and market data, publish to Kafka and record metrics.
 */
public final class EngineServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(EngineServer.class);

    record Shard(int id, EnginePipeline pipeline, PendingResponses responses, List<SymbolDefinition> symbols) {
    }

    private final EngineServerConfig config;
    private final Map<Integer, Shard> shardsBySymbol = new TreeMap<>();
    private final List<Shard> shards = new ArrayList<>();
    private final CopyOnWriteArrayList<Subscriber<EngineEvent>> eventSubscribers = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<MarketDataStreams.Session> marketDataSessions = new CopyOnWriteArrayList<>();
    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final List<KafkaEventPublisher> kafkaPublishers = new ArrayList<>();
    private final HealthStatusManager health = new HealthStatusManager();
    private final Server grpc;
    private MetricsHttpServer metricsServer;

    public EngineServer(EngineServerConfig config) {
        this(config, ServerBuilder.forPort(config.grpcPort()));
    }

    /** Builds on a caller-supplied server builder (tests use an in-process one). */
    public EngineServer(EngineServerConfig config, ServerBuilder<?> serverBuilder) {
        this.config = config;
        AtomicLong correlationIds = new AtomicLong();
        Map<Integer, List<SymbolDefinition>> byShard = new TreeMap<>();
        for (SymbolDefinition s : config.symbols()) {
            byShard.computeIfAbsent(s.shard(), k -> new ArrayList<>()).add(s);
        }
        for (var entry : byShard.entrySet()) {
            int shardId = entry.getKey();
            List<SymbolConfig> symbols = entry.getValue().stream().map(SymbolDefinition::toEngineConfig).toList();
            PendingResponses responses = new PendingResponses(shardId, correlationIds);
            List<OutboundListener> listeners = new ArrayList<>();
            listeners.add(responses);
            listeners.add(new EventStreams(shardId, eventSubscribers));
            listeners.add(new MarketDataStreams(shardId, marketDataSessions));
            if (config.kafkaEnabled()) {
                KafkaEventPublisher kafka = new KafkaEventPublisher(shardId, config.kafkaBootstrapServers(),
                        config.tradesTopic(), config.executionsTopic());
                kafkaPublishers.add(kafka);
                listeners.add(kafka);
            }
            MetricsHolder metricsHolder = new MetricsHolder();
            listeners.add(metricsHolder);
            PipelineConfig pipelineConfig = PipelineConfig.defaults(
                            new EngineConfig(shardId, config.maxOrdersPerShard(), symbols),
                            config.dataDirectory().resolve("shard-" + shardId))
                    .withWaitStrategies(config.waitStrategy(), config.waitStrategy())
                    .withFsync(config.fsync())
                    .withSnapshotInterval(config.snapshotInterval())
                    .withPretouchJournal(true);
            EnginePipeline pipeline = EnginePipeline.start(pipelineConfig, listeners);
            metricsHolder.delegate = new EngineMetrics(registry, shardId, pipeline);
            Shard shard = new Shard(shardId, pipeline, responses, entry.getValue());
            shards.add(shard);
            for (SymbolDefinition s : entry.getValue()) {
                shardsBySymbol.put(s.id(), shard);
            }
            LOG.info("shard {} started with symbols {}; recovery: {}", shardId,
                    entry.getValue().stream().map(SymbolDefinition::symbol).toList(), pipeline.recoveryInfo());
        }
        this.grpc = serverBuilder
                .addService(new MatchingEngineService(this))
                .addService(health.getHealthService())
                .addService(ProtoReflectionServiceV1.newInstance())
                .build();
    }

    public EngineServer start() {
        try {
            grpc.start();
            if (config.metricsPort() > 0) {
                metricsServer = new MetricsHttpServer(config.metricsPort(), registry, this::isHealthy);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        health.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
        LOG.info("engine gRPC on port {}, metrics on port {}", grpc.getPort(), config.metricsPort());
        return this;
    }

    Shard shardFor(int symbolId) {
        return shardsBySymbol.get(symbolId);
    }

    CopyOnWriteArrayList<Subscriber<EngineEvent>> eventSubscribers() {
        return eventSubscribers;
    }

    CopyOnWriteArrayList<MarketDataStreams.Session> marketDataSessions() {
        return marketDataSessions;
    }

    void requestImages() {
        for (Shard shard : shards) {
            for (SymbolDefinition s : shard.symbols()) {
                shard.pipeline().requestBookImage(s.id());
            }
        }
    }

    public boolean isHealthy() {
        return shards.stream().allMatch(s -> s.pipeline().isRunning());
    }

    public PrometheusMeterRegistry registry() {
        return registry;
    }

    public List<EnginePipeline> pipelines() {
        return shards.stream().map(Shard::pipeline).toList();
    }

    public int port() {
        return grpc.getPort();
    }

    @Override
    public void close() {
        health.enterTerminalState();
        grpc.shutdown();
        try {
            grpc.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (Shard shard : shards) {
            shard.pipeline().close();
        }
        if (metricsServer != null) {
            metricsServer.close();
        }
        LOG.info("engine stopped");
    }

    /** Lets the metrics listener be registered before the pipeline (and hence its gauges) exists. */
    private static final class MetricsHolder implements OutboundListener {
        volatile EngineMetrics delegate;

        @Override
        public void onEvent(dev.matchbook.engine.pipeline.OutboundEvent event, boolean endOfBatch) {
            EngineMetrics metrics = delegate;
            if (metrics != null) {
                metrics.onEvent(event, endOfBatch);
            }
        }
    }
}
