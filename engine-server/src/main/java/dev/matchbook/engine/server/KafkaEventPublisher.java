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
 * which removes duplicates caused by producer retries within a session. Duplicates caused by an engine
 * restart (recovery replays and re-emits events) are removed by consumers, keyed on the event's
 * {@code (shard, seq)} carried in the {@code mb-shard} and {@code mb-seq} headers. See ADR-0013.
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

    KafkaEventPublisher(int shard, String bootstrapServers, String tradesTopic, String executionsTopic) {
        this.shard = shard;
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
        switch (event.type()) {
            case TRADE -> send(tradesTopic, Integer.toString(event.symbolId()), event,
                    ProtoMapping.trade(event, shard).toByteArray());
            case EXECUTION_REPORT -> send(executionsTopic, Long.toString(event.clientId()), event,
                    ProtoMapping.executionReport(event, shard).toByteArray());
            default -> {
            }
        }
    }

    private void send(String topic, String key, OutboundEvent event, byte[] value) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("mb-shard", Integer.toString(shard).getBytes(StandardCharsets.US_ASCII));
        headers.add("mb-seq", Long.toString(event.seq()).getBytes(StandardCharsets.US_ASCII));
        headers.add("mb-journal-seq", Long.toString(event.journalSeq()).getBytes(StandardCharsets.US_ASCII));
        String traceparent = TraceContext.traceparent(event);
        if (traceparent != null) {
            headers.add("traceparent", traceparent.getBytes(StandardCharsets.US_ASCII));
        }
        producer.send(new ProducerRecord<>(topic, null, event.timestamp() / 1_000_000, key, value, headers),
                (metadata, error) -> {
                    if (error != null) {
                        if (sendErrors.incrementAndGet() % 1000 == 1) {
                            LOG.error("Kafka send failed (shard {}, seq {})", shard, event.seq(), error);
                        }
                    } else {
                        sent.incrementAndGet();
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
    }
}
