package dev.matchbook.engine.server;

import dev.matchbook.engine.core.ExecType;
import dev.matchbook.engine.core.Reason;
import dev.matchbook.engine.pipeline.EnginePipeline;
import dev.matchbook.engine.pipeline.OutboundEvent;
import dev.matchbook.engine.pipeline.OutboundListener;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Micrometer metrics for one shard, recorded on an outbound listener thread so the matching thread
 * never touches a meter.
 * <ul>
 *     <li>{@code matchbook_engine_commands_total{shard,result}}: command rate, by the command's first response</li>
 *     <li>{@code matchbook_engine_rejects_total{shard,reason}}: rejects by reason</li>
 *     <li>{@code matchbook_engine_trades_total{shard}}</li>
 *     <li>{@code matchbook_engine_latency_seconds{shard}}: histogram, from ingress (gRPC thread publishes
 *     into the ring) to the command's last event reaching this listener: ring hand-offs, journal, matching</li>
 *     <li>{@code matchbook_engine_match_seconds{shard}}: histogram, time inside the matching stage, measured
 *     on the matching thread and carried on the command's last event</li>
 *     <li>{@code matchbook_engine_queue_depth{shard,ring}}: inbound and outbound ring backlog</li>
 * </ul>
 */
final class EngineMetrics implements OutboundListener {

    private final Map<ExecType, Counter> commands = new EnumMap<>(ExecType.class);
    private final Map<Reason, Counter> rejects = new EnumMap<>(Reason.class);
    private final Counter trades;
    private final Timer latency;
    private final Timer matchTime;
    private final MeterRegistry registry;
    private final String shard;
    private boolean nextIsFirstOfCommand = true;

    EngineMetrics(MeterRegistry registry, int shardId, EnginePipeline pipeline) {
        this.registry = registry;
        this.shard = Integer.toString(shardId);
        this.trades = Counter.builder("matchbook.engine.trades").tag("shard", shard).register(registry);
        this.latency = Timer.builder("matchbook.engine.latency")
                .description("ingress to last event of the command, inside the engine process")
                .tag("shard", shard)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofNanos(500))
                .maximumExpectedValue(Duration.ofMillis(100))
                .register(registry);
        this.matchTime = Timer.builder("matchbook.engine.match")
                .description("time spent in the matching stage per command")
                .tag("shard", shard)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofNanos(50))
                .maximumExpectedValue(Duration.ofMillis(10))
                .register(registry);
        Gauge.builder("matchbook.engine.queue.depth", pipeline, EnginePipeline::inboundDepth)
                .tag("shard", shard).tag("ring", "inbound").register(registry);
        Gauge.builder("matchbook.engine.queue.depth", pipeline, EnginePipeline::outboundDepth)
                .tag("shard", shard).tag("ring", "outbound").register(registry);
        Gauge.builder("matchbook.engine.journal.seq", pipeline, EnginePipeline::lastJournalSeq)
                .tag("shard", shard).register(registry);
        Gauge.builder("matchbook.engine.snapshot.pause", pipeline, p -> p.metrics().lastSnapshotPauseNanos() / 1e9)
                .baseUnit("seconds").tag("shard", shard).register(registry);
        Gauge.builder("matchbook.engine.recovery.duration", pipeline, p -> p.recoveryInfo().recoveryNanos() / 1e9)
                .baseUnit("seconds").tag("shard", shard).register(registry);
    }

    @Override
    public void onEvent(OutboundEvent event, boolean endOfBatch) {
        boolean first = nextIsFirstOfCommand;
        nextIsFirstOfCommand = event.lastOfCommand();
        switch (event.type()) {
            case EXECUTION_REPORT -> {
                // The first event of every command is its response; count commands by that response.
                if (first) {
                    commands.computeIfAbsent(event.execType(), t -> Counter.builder("matchbook.engine.commands")
                            .tag("shard", shard).tag("result", t.name()).register(registry)).increment();
                }
                if (event.execType() == ExecType.REJECTED || event.execType() == ExecType.CANCEL_REJECTED
                        || event.execType() == ExecType.REPLACE_REJECTED) {
                    rejects.computeIfAbsent(event.reason(), r -> Counter.builder("matchbook.engine.rejects")
                            .tag("shard", shard).tag("reason", r.name()).register(registry)).increment();
                }
            }
            case TRADE -> trades.increment();
            default -> {
            }
        }
        if (event.lastOfCommand()) {
            if (event.ingressNanos() != 0) {
                latency.record(System.nanoTime() - event.ingressNanos(), TimeUnit.NANOSECONDS);
            }
            if (event.matchEndNanos() != 0) {
                matchTime.record(event.matchEndNanos() - event.matchNanos(), TimeUnit.NANOSECONDS);
            }
        }
    }
}
