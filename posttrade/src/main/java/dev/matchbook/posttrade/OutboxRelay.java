package dev.matchbook.posttrade;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes outbox rows to Kafka.
 *
 * <p>Each tick, in one transaction: lock up to {@value #BATCH} unpublished rows with
 * {@code FOR UPDATE SKIP LOCKED} (so several relay instances can run without blocking each other or
 * publishing the same row at the same time), send them, wait for the broker's acknowledgements, then mark
 * them published. If anything fails, the transaction rolls back and the rows are sent again next tick:
 * delivery is at-least-once, and each record carries its outbox id ({@code mb-outbox-id}) and the
 * position {@code version} so consumers can discard duplicates and out-of-order updates.
 */
@Component
public class OutboxRelay {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxRelay.class);
    static final int BATCH = 500;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final KafkaTemplate<String, String> kafka;
    private final Counter published;
    private final AtomicLong backlog = new AtomicLong();

    public OutboxRelay(JdbcTemplate jdbc, TransactionTemplate tx, KafkaTemplate<String, String> kafka,
                       MeterRegistry meters) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.kafka = kafka;
        this.published = Counter.builder("matchbook.posttrade.outbox.published").register(meters);
        Gauge.builder("matchbook.posttrade.outbox.backlog", backlog, AtomicLong::get).register(meters);
    }

    @Scheduled(fixedDelayString = "${matchbook.outbox.poll-interval:100}")
    public void relay() {
        try {
            int sent;
            do {
                Integer count = tx.execute(status -> publishBatch());
                sent = count == null ? 0 : count;
            } while (sent == BATCH);
            Long remaining = jdbc.queryForObject("select count(*) from outbox where published_at is null", Long.class);
            backlog.set(remaining == null ? 0 : remaining);
        } catch (RuntimeException e) {
            LOG.warn("outbox relay failed, will retry: {}", e.toString());
        }
    }

    private int publishBatch() {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                select id, topic, msg_key, payload::text as payload from outbox
                where published_at is null order by id limit ? for update skip locked""", BATCH);
        if (rows.isEmpty()) {
            return 0;
        }
        List<CompletableFuture<?>> sends = new ArrayList<>(rows.size());
        List<Long> ids = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            long id = ((Number) row.get("id")).longValue();
            ProducerRecord<String, String> record = new ProducerRecord<>((String) row.get("topic"),
                    (String) row.get("msg_key"), (String) row.get("payload"));
            record.headers().add("mb-outbox-id", Long.toString(id).getBytes(StandardCharsets.US_ASCII));
            sends.add(kafka.send(record));
            ids.add(id);
        }
        try {
            CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("outbox publish failed", e);
        }
        jdbc.batchUpdate("update outbox set published_at = now() where id = ?", ids, ids.size(),
                (ps, id) -> ps.setLong(1, id));
        published.increment(ids.size());
        return rows.size();
    }

    /** Published rows are kept for a day for auditing, then removed. */
    @Scheduled(fixedDelay = 3_600_000, initialDelay = 60_000)
    public void cleanUp() {
        int removed = jdbc.update("delete from outbox where published_at < now() - interval '1 day'");
        if (removed > 0) {
            LOG.info("removed {} published outbox rows", removed);
        }
    }
}
