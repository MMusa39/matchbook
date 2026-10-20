package dev.matchbook.engine.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaAckTrackerTest {

    @TempDir
    Path dir;

    @Test
    void floorAdvancesOnlyOverFullyAcknowledgedCommandsInOrder() {
        KafkaAckTracker t = new KafkaAckTracker(dir.resolve("acked"));
        t.sending(1);
        t.sending(1);
        t.commandSent(1);
        t.sending(2);
        t.commandSent(2);
        t.sending(3);
        t.commandSent(3);

        t.acked(2);
        t.acked(3);
        assertThat(t.ackedFloor()).as("command 1 still has records in flight").isZero();
        t.acked(1);
        assertThat(t.ackedFloor()).isEqualTo(0);
        t.acked(1);
        assertThat(t.ackedFloor()).isEqualTo(3);
    }

    @Test
    void aCommandIsNotCompleteUntilAllItsRecordsWereSent() {
        KafkaAckTracker t = new KafkaAckTracker(dir.resolve("acked"));
        t.sending(5);
        t.acked(5); // first record acknowledged before the second is sent
        assertThat(t.ackedFloor()).isZero();
        t.sending(5);
        t.commandSent(5);
        assertThat(t.ackedFloor()).isZero();
        t.acked(5);
        assertThat(t.ackedFloor()).isEqualTo(5);
    }

    @Test
    void persistsAndReloadsTheFloor() {
        Path file = dir.resolve("acked");
        KafkaAckTracker t = new KafkaAckTracker(file);
        t.sending(7);
        t.commandSent(7);
        t.acked(7);
        t.persist();
        assertThat(new KafkaAckTracker(file).persistedFloor()).isEqualTo(7);
    }
}
