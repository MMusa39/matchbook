package dev.matchbook.engine.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

/**
 * Tracks which journal positions have been fully delivered to Kafka, so the journal can act as the
 * engine's outbox (ADR-0013).
 *
 * <p>Every journaled command produces at least one Kafka record. Records are sent in journal order but
 * acknowledged asynchronously, in any order. The tracker keeps, per in-flight journal sequence, how many
 * of its records are still unacknowledged; the <em>acked floor</em> is the highest sequence {@code J}
 * such that every record of every command up to {@code J} is acknowledged. The floor is persisted to a
 * small file, and on restart the engine recovers from the newest snapshot at or below it, so everything
 * Kafka might not have is replayed and re-sent. Consumers de-duplicate the re-sent records by
 * {@code (shard, seq)}.
 */
final class KafkaAckTracker {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAckTracker.class);

    private final Path file;
    private final ArrayDeque<Long> order = new ArrayDeque<>();
    private final Map<Long, int[]> outstanding = new HashMap<>();
    private long ackedFloor;
    private volatile long persistedFloor;
    private long lastSentSeq;
    /** Highest journal sequence whose records have all been handed to the producer. */
    private long sentThrough;

    KafkaAckTracker(Path file) {
        this.file = file;
        long loaded = 0;
        if (Files.exists(file)) {
            try {
                loaded = Long.parseLong(Files.readString(file).trim());
            } catch (IOException | NumberFormatException e) {
                LOG.warn("could not read {}; replaying from the start of the retained journal", file, e);
            }
        }
        this.ackedFloor = loaded;
        this.persistedFloor = loaded;
        this.lastSentSeq = loaded;
        this.sentThrough = loaded;
    }

    /** The floor as last persisted: what a restart right now would replay from. */
    long persistedFloor() {
        return persistedFloor;
    }

    synchronized long ackedFloor() {
        return ackedFloor;
    }

    /** Called on the publisher thread before a record for {@code journalSeq} is sent. */
    synchronized void sending(long journalSeq) {
        if (journalSeq <= ackedFloor) {
            return; // a replay of something already delivered
        }
        int[] count = outstanding.get(journalSeq);
        if (count == null) {
            outstanding.put(journalSeq, new int[]{1});
            order.addLast(journalSeq);
        } else {
            count[0]++;
        }
        lastSentSeq = Math.max(lastSentSeq, journalSeq);
    }

    /**
     * Called on the publisher thread after the last event of the command with {@code journalSeq}: no more
     * records will be sent for it, so once they are all acknowledged it may leave the in-flight set.
     * Without this, a command whose first record was acknowledged before its second was sent would look
     * complete too early.
     */
    synchronized void commandSent(long journalSeq) {
        if (journalSeq > sentThrough) {
            sentThrough = journalSeq;
            advance();
        }
    }

    /** Called from the producer's callback thread when a record for {@code journalSeq} is acknowledged. */
    synchronized void acked(long journalSeq) {
        int[] count = outstanding.get(journalSeq);
        if (count == null) {
            return;
        }
        count[0]--;
        advance();
    }

    private void advance() {
        while (!order.isEmpty()) {
            long head = order.peekFirst();
            int[] headCount = outstanding.get(head);
            if (headCount[0] > 0 || head > sentThrough) {
                break;
            }
            order.pollFirst();
            outstanding.remove(head);
            ackedFloor = head;
        }
        if (order.isEmpty() && sentThrough > ackedFloor) {
            // Every command up to sentThrough is fully acknowledged (commands with no Kafka records included).
            ackedFloor = sentThrough;
        }
    }

    /**
     * Persists the floor (temp file + atomic rename). Called periodically. The floor only covers commands
     * whose records were all <em>sent</em> before the last batch boundary; a command whose records were
     * not all sent yet keeps its sequence in flight.
     */
    void persist() {
        long floor = ackedFloor();
        if (floor == persistedFloor) {
            return;
        }
        try {
            Path temp = file.resolveSibling(file.getFileName() + ".tmp");
            Files.writeString(temp, Long.toString(floor));
            Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            persistedFloor = floor;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
