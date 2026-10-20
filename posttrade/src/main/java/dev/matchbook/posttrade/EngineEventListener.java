package dev.matchbook.posttrade;

import com.google.protobuf.InvalidProtocolBufferException;
import dev.matchbook.proto.v1.ExecutionReport;
import dev.matchbook.proto.v1.Trade;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Consumes the engine's Kafka topics in batches. Each poll (up to {@code max.poll.records}) is applied in
 * one database transaction, and offsets are acknowledged only after it commits: at-least-once delivery
 * into an idempotent processor, with one commit per batch instead of one per record.
 *
 * <p>Applying one record per transaction capped the drop copy at about 140 executions/s in Docker, the
 * cost of one synchronous Postgres commit each (RESULTS §6).
 *
 * <p>A record that cannot be parsed is reported by its index ({@link BatchListenerFailedException}), so the
 * error handler commits everything before it, sends it to the dead-letter topic and redelivers the rest.
 */
@Component
public class EngineEventListener {

    private final TradeProcessor processor;

    public EngineEventListener(TradeProcessor processor) {
        this.processor = processor;
    }

    @KafkaListener(id = "trades", topics = "${matchbook.trades-topic:matchbook.trades}", batch = "true")
    public void onTrades(List<ConsumerRecord<String, byte[]>> records, Acknowledgment ack) {
        List<TradeProcessor.Event<Trade>> events = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, byte[]> record = records.get(i);
            try {
                EngineEventHeaders h = EngineEventHeaders.of(record);
                events.add(new TradeProcessor.Event<>(Trade.parseFrom(record.value()), h.shard(), h.seq()));
            } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
                throw new BatchListenerFailedException("unparseable trade record", e, i);
            }
        }
        processor.applyTrades(events);
        ack.acknowledge();
    }

    @KafkaListener(id = "executions", topics = "${matchbook.executions-topic:matchbook.executions}", batch = "true")
    public void onExecutions(List<ConsumerRecord<String, byte[]>> records, Acknowledgment ack) {
        List<TradeProcessor.Event<ExecutionReport>> events = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, byte[]> record = records.get(i);
            try {
                EngineEventHeaders h = EngineEventHeaders.of(record);
                events.add(new TradeProcessor.Event<>(ExecutionReport.parseFrom(record.value()), h.shard(), h.seq()));
            } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
                throw new BatchListenerFailedException("unparseable execution record", e, i);
            }
        }
        processor.applyExecutions(events);
        ack.acknowledge();
    }
}
