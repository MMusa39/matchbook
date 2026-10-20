package dev.matchbook.posttrade;

import dev.matchbook.proto.v1.ExecType;
import dev.matchbook.proto.v1.ExecutionReport;
import dev.matchbook.proto.v1.Side;
import dev.matchbook.proto.v1.Trade;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "matchbook.outbox.poll-interval=50")
class PostTradeIT {

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine");
    static KafkaProducer<String, byte[]> producer;

    static {
        KAFKA.start();
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @BeforeAll
    static void createProducer() {
        producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
                new StringSerializer(), new ByteArraySerializer());
    }

    @AfterAll
    static void closeProducer() {
        producer.close();
    }

    @Autowired
    JdbcTemplate jdbc;

    static void send(String topic, int shard, long seq, byte[] value) throws Exception {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, "k", value);
        record.headers().add("mb-shard", Integer.toString(shard).getBytes(StandardCharsets.US_ASCII));
        record.headers().add("mb-seq", Long.toString(seq).getBytes(StandardCharsets.US_ASCII));
        producer.send(record).get();
    }

    static Trade trade(long tradeId, long buyer, long seller, long price, long qty) {
        return Trade.newBuilder().setTradeId(tradeId).setSymbolId(1).setPrice(price).setQuantity(qty)
                .setAggressorSide(Side.SIDE_BUY).setBuyOrderId(tradeId * 10).setSellOrderId(tradeId * 10 + 1)
                .setBuyClientId(buyer).setSellClientId(seller).setTimestamp(1).build();
    }

    static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!condition.getAsBoolean()) {
            assertThat(System.currentTimeMillis()).as("condition within 30s").isLessThan(deadline);
            Thread.sleep(50);
        }
    }

    long position(long client) {
        List<Long> q = jdbc.queryForList("select quantity from positions where client_id = ? and symbol_id = 1",
                Long.class, client);
        return q.isEmpty() ? 0 : q.getFirst();
    }

    @Test
    void tradesBuildPositionsAndPnlForBothSides() throws Exception {
        send("matchbook.trades", 0, 1_001, trade(5_001, 11, 12, 15_000, 100).toByteArray());
        send("matchbook.trades", 0, 1_002, trade(5_002, 12, 11, 15_500, 40).toByteArray());
        await(() -> position(11) == 60);

        Map<String, Object> buyer = jdbc.queryForMap(
                "select quantity, avg_price, realized_pnl from position_report where client_id = 11");
        assertThat(buyer.get("quantity")).isEqualTo(60L);
        assertThat((BigDecimal) buyer.get("avg_price")).isEqualByComparingTo("150.00");
        assertThat((BigDecimal) buyer.get("realized_pnl")).isEqualByComparingTo("200.00"); // 40 x 5.00
        assertThat(position(12)).isEqualTo(-60);
    }

    @Test
    void duplicateDeliveryIsAppliedOnce() throws Exception {
        byte[] t = trade(6_001, 21, 22, 15_000, 10).toByteArray();
        send("matchbook.trades", 1, 2_001, t);
        send("matchbook.trades", 1, 2_001, t); // same (shard, seq): an engine replay or producer retry
        send("matchbook.trades", 1, 2_002, trade(6_002, 21, 22, 15_000, 1).toByteArray());
        await(() -> position(21) == 11);
        Thread.sleep(500);
        assertThat(position(21)).isEqualTo(11);
        assertThat(jdbc.queryForObject("select count(*) from trades where trade_id = 6001", Long.class)).isEqualTo(1);
    }

    @Test
    void positionChangesArePublishedThroughTheOutbox() throws Exception {
        send("matchbook.trades", 2, 3_001, trade(7_001, 31, 32, 15_000, 5).toByteArray());
        await(() -> position(31) == 5);
        await(() -> jdbc.queryForObject("select count(*) from outbox where published_at is null", Long.class) == 0);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-positions",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"), new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("matchbook.positions"));
            List<ConsumerRecord<String, String>> seen = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (seen.stream().noneMatch(r -> r.key().equals("31")) && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(seen::add);
            }
            ConsumerRecord<String, String> forBuyer = seen.stream().filter(r -> r.key().equals("31")).findFirst().orElseThrow();
            // jsonb normalises spacing and key order
            assertThat(forBuyer.value()).containsPattern("\"quantity\":\\s*5\\b").containsPattern("\"tradeId\":\\s*7001");
            assertThat(forBuyer.headers().lastHeader("mb-outbox-id")).isNotNull();
        }
    }

    @Test
    void executionReportsAreStoredAsADropCopy() throws Exception {
        ExecutionReport report = ExecutionReport.newBuilder().setExecType(ExecType.EXEC_TYPE_NEW).setReason("NONE")
                .setOrderId(9_001).setClientId(41).setClOrdId(1).setSymbolId(1).setSide(Side.SIDE_BUY)
                .setPrice(15_000).setQuantity(10).setLeavesQuantity(10).build();
        send("matchbook.executions", 3, 4_001, report.toByteArray());
        send("matchbook.executions", 3, 4_001, report.toByteArray());
        await(() -> jdbc.queryForObject("select count(*) from executions where order_id = 9001", Long.class) == 1);
    }

    @Test
    void unparseableRecordsGoToTheDeadLetterTopicWithoutBlockingThePartition() throws Exception {
        send("matchbook.trades", 4, 5_001, new byte[]{1, 2, 3, 4, 5});
        send("matchbook.trades", 4, 5_002, trade(8_001, 51, 52, 15_000, 3).toByteArray());
        await(() -> position(51) == 3);

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-dlt",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"), new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of("matchbook.trades.DLT"));
            List<ConsumerRecord<String, byte[]>> dead = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (dead.isEmpty() && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(dead::add);
            }
            assertThat(dead).isNotEmpty();
        }
    }
}
