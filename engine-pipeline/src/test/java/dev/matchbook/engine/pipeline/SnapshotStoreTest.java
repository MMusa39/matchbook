package dev.matchbook.engine.pipeline;

import dev.matchbook.engine.core.Command;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotStoreTest {

    @TempDir
    Path dir;

    @Test
    void keepsTheNewestSnapshotsAndLoadsTheLatest() throws IOException {
        SnapshotStore store = new SnapshotStore(dir, 3);
        for (long seq = 10; seq <= 50; seq += 10) {
            store.write(new Snapshot(seq, new byte[]{(byte) seq, 1, 2, 3}, 0));
        }
        assertThat(snapshotFiles()).hasSize(3);
        Snapshot latest = store.latest().orElseThrow();
        assertThat(latest.journalSeq()).isEqualTo(50);
        assertThat(latest.state()).containsExactly(50, 1, 2, 3);
    }

    @Test
    void fallsBackToAnOlderSnapshotWhenTheNewestIsCorrupt() throws IOException {
        SnapshotStore store = new SnapshotStore(dir, 3);
        store.write(new Snapshot(10, new byte[]{1, 2, 3, 4}, 0));
        store.write(new Snapshot(20, new byte[]{5, 6, 7, 8}, 0));
        Path newest = snapshotFiles().getLast();
        byte[] bytes = Files.readAllBytes(newest);
        bytes[bytes.length - 1] ^= 0x55;
        Files.write(newest, bytes);

        assertThat(store.latest().orElseThrow().journalSeq()).isEqualTo(10);
    }

    @Test
    void emptyDirectoryHasNoSnapshot() {
        assertThat(new SnapshotStore(dir, 3).latest()).isEmpty();
    }

    @Test
    void deletesJournalSegmentsCoveredByTheOldestKeptSnapshot() {
        Command command = new Command();
        SyntheticOrderFlow flow = new SyntheticOrderFlow(1, 256);
        try (Journal journal = Journal.open(dir, 64 * 1024, FsyncPolicy.NONE)) {
            for (int i = 1; i <= 4_000; i++) {
                journal.append(flow.next(command, i));
            }
        }
        int before = JournalFormat.segments(dir).size();
        SnapshotStore store = new SnapshotStore(dir, 1);
        store.write(new Snapshot(3_000, new byte[]{1}, 0));

        assertThat(JournalFormat.segments(dir).size()).isLessThan(before);
        // Everything after the snapshot is still replayable.
        long[] first = {0};
        JournalReader.replay(dir, 3_000, (seq, c) -> {
            if (first[0] == 0) {
                first[0] = seq;
            }
        });
        assertThat(first[0]).isEqualTo(3_001);
    }

    private java.util.List<Path> snapshotFiles() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.toString().endsWith(".snap")).sorted().toList();
        }
    }
}
