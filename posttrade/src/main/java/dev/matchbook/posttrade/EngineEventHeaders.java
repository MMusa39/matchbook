package dev.matchbook.posttrade;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;

/** Reads the engine's de-duplication headers ({@code mb-shard}, {@code mb-seq}) from a Kafka record. */
record EngineEventHeaders(int shard, long seq) {

    static EngineEventHeaders of(ConsumerRecord<?, ?> record) {
        return new EngineEventHeaders(Integer.parseInt(header(record, "mb-shard")),
                Long.parseLong(header(record, "mb-seq")));
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        if (h == null) {
            throw new IllegalArgumentException("record without " + name + " header: " + record.topic()
                    + "-" + record.partition() + "@" + record.offset());
        }
        return new String(h.value(), StandardCharsets.US_ASCII);
    }
}
