package dev.matchbook.engine.server;

import dev.matchbook.engine.pipeline.OutboundEvent;
import dev.matchbook.engine.pipeline.OutboundListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes trades and execution reports to Kafka for post-trade processing.
 *
 * <p>Delivery is at-least-once. The producer is idempotent ({@code enable.idempotence}, {@code acks=all}),
 * which removes duplicates caused by producer retries within a session. Across engine restarts,
 * {@link KafkaAckTracker} records how far Kafka has acknowledged, and recovery replays (re-sends) from
 * there; consumers remove those duplicates by the event's {@code (shard, seq)}, carried in the
 * {@code mb-shard} and {@code mb-seq} headers. See ADR-0013.
 *
 * <p>Records are keyed so that ordering holds where it matters: executions by client id (one client's
 * reports stay in order on one partition), trades by symbol id.
 */
final class KafkaEventPublisher implements OutboundListener {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final int shard;
    private final String tradesTopic;
    private final String executionsTopic;
    private final KafkaProducer<String, byte[]> producer;
    private final AtomicLong sendErrors = new AtomicLong();
    private final AtomicLong sent = new AtomicLong();
    private final KafkaAckTracker tracker;
    private final EngineTracing tracing;
    private io.opentelemetry.api.trace.Span commandSpan;
    private String commandTraceparent;
    private boolean firstOfCommand = true;

    KafkaEventPublisher(int shard, String bootstrapServers, String tradesTopic, String executionsTopic,
                        KafkaAckTracker tracker, EngineTracing tracing) {
        this.shard = shard;
        this.tracker = tracker;
        this.tracing = tracing;
        this.tradesTopic = tradesTopic;
        this.executionsTopic = executionsTopic;
        this.producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.CLIENT_ID_CONFIG, "engine-shard-" + shard,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.LINGER_MS_CONFIG, 2,
                ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4",
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000),
                new StringSerializer(), new ByteArraySerializer());
    }

    @Override
    public void onEvent(OutboundEvent event, boolean endOfBatch) {
        if (firstOfCommand) {
            commandSpan = tracing.start(event, shard);
            commandTraceparent = commandSpan != null ? EngineTracing.traceparent(commandSpan)
                    : TraceContext.traceparent(event);
        }
        firstOfCommand = event.lastOfCommand();
        switch (event.type()) {
            case TRADE -> send(tradesTopic, Integer.toString(event.symbolId()), event,
                    ProtoMapping.trade(event, shard).toByteArray());
            case EXECUTION_REPORT -> send(executionsTopic, Long.toString(event.clientId()), event,
                    ProtoMapping.executionReport(event, shard).toByteArray());
            default -> {
            }
        }
        if (event.lastOfCommand()) {
            if (event.journalSeq() > 0) {
                tracker.commandSent(event.journalSeq());
            }
            if (commandSpan != null) {
                tracing.end(commandSpan);
                commandSpan = null;
            }
        }
    }

    private void send(String topic, String key, OutboundEvent event, byte[] value) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("mb-shard", Integer.toString(shard).getBytes(StandardCharsets.US_ASCII));
        headers.add("mb-seq", Long.toString(event.seq()).getBytes(StandardCharsets.US_ASCII));
        headers.add("mb-journal-seq", Long.toString(event.journalSeq()).getBytes(StandardCharsets.US_ASCII));
        long journalSeq = event.journalSeq();
        tracker.sending(journalSeq);
        // The engine span when tracing is exported, otherwise the caller's own context passes through.
        if (commandTraceparent != null) {
            headers.add("traceparent", commandTraceparent.getBytes(StandardCharsets.US_ASCII));
        }
        producer.send(new ProducerRecord<>(topic, null, event.timestamp() / 1_000_000, key, value, headers),
                (metadata, error) -> {
                    if (error != null) {
                        // Not acknowledged: the acked floor stays below this command, so a restart re-sends it.
                        if (sendErrors.incrementAndGet() % 1000 == 1) {
                            LOG.error("Kafka send failed (shard {}, journal seq {})", shard, journalSeq, error);
                        }
                    } else {
                        sent.incrementAndGet();
                        tracker.acked(journalSeq);
                    }
                });
    }

    long sendErrors() {
        return sendErrors.get();
    }

    long sent() {
        return sent.get();
    }

    @Override
    public void onShutdown() {
        producer.flush();
        producer.close();
        tracker.persist();
    }
}
