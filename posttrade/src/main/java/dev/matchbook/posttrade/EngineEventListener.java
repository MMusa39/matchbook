package dev.matchbook.posttrade;

import com.google.protobuf.InvalidProtocolBufferException;
import dev.matchbook.proto.v1.ExecutionReport;
import dev.matchbook.proto.v1.Trade;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes the engine's Kafka topics. Offsets are acknowledged only after the database transaction has
 * committed (manual acknowledgement), giving at-least-once delivery into an idempotent processor.
 */
@Component
public class EngineEventListener {

    private final TradeProcessor processor;

    public EngineEventListener(TradeProcessor processor) {
        this.processor = processor;
    }

    @KafkaListener(id = "trades", topics = "${matchbook.trades-topic:matchbook.trades}")
    public void onTrade(ConsumerRecord<String, byte[]> record, Acknowledgment ack) throws InvalidProtocolBufferException {
        EngineEventHeaders h = EngineEventHeaders.of(record);
        processor.applyTrade(Trade.parseFrom(record.value()), h.shard(), h.seq());
        ack.acknowledge();
    }

    @KafkaListener(id = "executions", topics = "${matchbook.executions-topic:matchbook.executions}")
    public void onExecution(ConsumerRecord<String, byte[]> record, Acknowledgment ack)
            throws InvalidProtocolBufferException {
        EngineEventHeaders h = EngineEventHeaders.of(record);
        processor.applyExecution(ExecutionReport.parseFrom(record.value()), h.shard(), h.seq());
        ack.acknowledge();
    }
}
