package dev.matchbook.engine.pipeline;

import dev.matchbook.engine.core.Command;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class JournalTest {

    private static final long SMALL_SEGMENT = 64 * 1024;

    @TempDir
    Path dir;

    private final SyntheticOrderFlow flow = new SyntheticOrderFlow(7, 512);
    private final Command command = new Command();

    private List<String> append(Journal journal, int count) {
        List<String> written = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            flow.next(command, i + 1);
            journal.append(command);
            written.add(command.toString());
        }
        return written;
    }

    private static List<String> replay(Path dir, long afterSeq) {
        List<String> read = new ArrayList<>();
        JournalReader.replay(dir, afterSeq, (seq, c) -> read.add(c.toString()));
        return read;
    }

    @Test
    void replaysExactlyWhatWasAppended() {
        List<String> written;
        try (Journal journal = Journal.open(dir, SMALL_SEGMENT, FsyncPolicy.NONE)) {
            written = append(journal, 500);
            assertThat(journal.lastSeq()).isEqualTo(500);
        }
        assertThat(replay(dir, 0)).isEqualTo(written);
        assertThat(replay(dir, 450)).isEqualTo(written.subList(450, 500));
    }

    @Test
    void rollsAcrossSegmentsAndReplaysInOrder() {
        List<String> written;
        try (Journal journal = Journal.open(dir, SMALL_SEGMENT, FsyncPolicy.NONE)) {
            written = append(journal, 5_000);
        }
        assertThat(JournalFormat.segments(dir)).hasSizeGreaterThan(5);
        assertThat(replay(dir, 0)).isEqualTo(written);
        JournalReader.ScanResult scan = JournalReader.replay(dir, 0, (s, c) -> { });
        assertThat(scan.lastSeq()).isEqualTo(5_000);
        assertThat(scan.torn()).isFalse();
    }

    @Test
    void reopenAppendsAfterTheLastRecord() {
        List<String> written = new ArrayList<>();
        for (int round = 0; round < 3; round++) {
            try (Journal journal = Journal.open(dir, SMALL_SEGMENT, FsyncPolicy.NONE)) {
                written.addAll(append(journal, 700));
            }
        }
        assertThat(replay(dir, 0)).isEqualTo(written);
    }

    @Test
    void recordTornByAProcessCrashReadsAsEndOfJournal() throws IOException {
        try (Journal journal = Journal.open(dir, SMALL_SEGMENT, FsyncPolicy.NONE)) {
            append(journal, 10);
        }
        // The writer stores the length last; a crash before that leaves length == 0.
        Path segment = JournalFormat.segments(dir).getFirst();
        writeInt(segment, 9L * JournalFormat.RECORD_LENGTH, 0);

        JournalReader.ScanResult scan = JournalReader.replay(dir, 0, (s, c) -> { });
        assertThat(scan.lastSeq()).isEqualTo(9);
        assertThat(scan.torn()).isFalse();
    }

    @Test
    void recordWithBadChecksumStopsReplayAndIsOverwrittenOnReopen() throws IOException {
        List<String> written;
        try (Journal journal = Journal.open(dir, SMALL_SEGMENT, FsyncPolicy.NONE)) {
            written = append(journal, 10);
        }
        Path segment = JournalFormat.segments(dir).getFirst();
        // Flip a payload byte of record 8 (index 7): pages persisted out of order after power loss.
        try (RandomAccessFile file = new RandomAccessFile(segment.toFile(), "rw")) {
            long at = 7L * JournalFormat.RECORD_LENGTH + JournalFormat.HEADER_LENGTH + 20;
            file.seek(at);
            int b = file.read();
            file.seek(at);
            file.write(b ^ 0xFF);
        }
        JournalReader.ScanResult scan = JournalReader.replay(dir, 0, (s, c) -> { });
        assertThat(scan.lastSeq()).isEqualTo(7);
        assertThat(scan.torn()).isTrue();

        List<String> after;
        try (Journal journal = Journal.open(dir, SMALL_SEGMENT, FsyncPolicy.NONE)) {
            assertThat(journal.lastSeq()).isEqualTo(7);
            after = append(journal, 3);
        }
        List<String> expected = new ArrayList<>(written.subList(0, 7));
        expected.addAll(after);
        assertThat(replay(dir, 0)).isEqualTo(expected);
    }

    @Test
    void snapshotAheadOfJournalStartsAFreshJournal() throws IOException {
        try (Journal journal = Journal.open(dir, SMALL_SEGMENT, FsyncPolicy.NONE)) {
            append(journal, 10);
        }
        List<String> after;
        try (Journal journal = Journal.open(dir, SMALL_SEGMENT, FsyncPolicy.NONE, 25)) {
            assertThat(journal.lastSeq()).isEqualTo(25);
            after = append(journal, 2);
        }
        assertThat(replay(dir, 25)).isEqualTo(after);
        assertThat(JournalReader.replay(dir, 0, (s, c) -> { }).lastSeq()).isEqualTo(27);
        try (var files = Files.list(dir)) {
            assertThat(files.map(p -> p.getFileName().toString())).anyMatch(n -> n.endsWith(".stale"));
        }
    }

    @Test
    void everyBatchPolicyFlushesOncePerBatch() {
        try (Journal journal = Journal.open(dir, SMALL_SEGMENT, FsyncPolicy.EVERY_BATCH)) {
            append(journal, 5);
            journal.endOfBatch();
            journal.endOfBatch();
            append(journal, 5);
            journal.endOfBatch();
            assertThat(journal.flushes()).isEqualTo(2);
        }
    }

    private static void writeInt(Path file, long offset, int value) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(offset);
            raf.write(new byte[]{(byte) value, (byte) (value >>> 8), (byte) (value >>> 16), (byte) (value >>> 24)});
        }
    }
}
