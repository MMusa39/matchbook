package dev.matchbook.engine.server;

import dev.matchbook.proto.v1.ExecutionReport;
import dev.matchbook.proto.v1.MatchingEngineGrpc;
import dev.matchbook.proto.v1.Side;
import dev.matchbook.proto.v1.Trade;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class KafkaPublishingIT {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @TempDir
    Path dir;

    @Test
    void publishesTradesAndExecutionsWithDedupHeaders() throws Exception {
        String name = InProcessServerBuilder.generateName();
        try (EngineServer server = new EngineServer(EngineServerTest.config(dir, KAFKA.getBootstrapServers()),
                InProcessServerBuilder.forName(name).directExecutor()).start()) {
            ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            var engine = MatchingEngineGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
            engine.submit(EngineServerTest.order(1, 1, Side.SIDE_SELL, 15_000, 10)
                    .toBuilder().setTraceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01").build());
            engine.submit(EngineServerTest.order(2, 1, Side.SIDE_BUY, 15_000, 10));
            channel.shutdownNow();

            List<ConsumerRecord<String, byte[]>> trades = consume("matchbook.trades", 1);
            Trade trade = Trade.parseFrom(trades.getFirst().value());
            assertThat(trade.getQuantity()).isEqualTo(10);
            assertThat(trades.getFirst().key()).isEqualTo("1");
            assertThat(header(trades.getFirst(), "mb-shard")).isEqualTo("0");
            assertThat(header(trades.getFirst(), "mb-seq")).isEqualTo(Long.toString(trade.getSeq()));

            List<ConsumerRecord<String, byte[]>> executions = consume("matchbook.executions", 4);
            List<ExecutionReport> reports = new ArrayList<>();
            for (var r : executions) {
                reports.add(ExecutionReport.parseFrom(r.value()));
            }
            assertThat(reports).extracting(ExecutionReport::getClientId).contains(1L, 2L);
            assertThat(executions).filteredOn(r -> r.key().equals("1"))
                    .first().satisfies(r -> assertThat(header(r, "traceparent"))
                            .isEqualTo("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"));
        }
    }

    private static List<ConsumerRecord<String, byte[]>> consume(String topic, int count) {
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + topic,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of(topic));
            List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (out.size() < count && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(out::add);
            }
            assertThat(out).hasSizeGreaterThanOrEqualTo(count);
            return out;
        }
    }

    private static String header(ConsumerRecord<String, byte[]> record, String key) {
        var h = record.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.US_ASCII);
    }
}
