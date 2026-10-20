package dev.matchbook.engine.pipeline;

import dev.matchbook.engine.core.Command;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.CRC32C;

/** Reads journal segments in order, validating every record. Used for recovery and by {@link Journal#open}. */
public final class JournalReader {

    /** Receives each valid record in sequence order. The command is reused between calls. */
    @FunctionalInterface
    public interface Visitor {
        void onRecord(long journalSeq, Command command);
    }

    /**
     * Where a scan ended.
     *
     * @param lastSeq      sequence of the last valid record, or 0 if there are none
     * @param records      number of records visited (those after {@code afterSeq})
     * @param lastSegment  the segment holding the end of the journal, or {@code null} if there are none
     * @param endOffset    byte offset in {@code lastSegment} where the next record goes
     * @param torn         true if the scan stopped at a damaged record rather than a clean end
     */
    public record ScanResult(long lastSeq, long records, Path lastSegment, long endOffset, boolean torn) {
    }

    private JournalReader() {
    }

    /** Visits every valid record with a sequence number greater than {@code afterSeq}. */
    public static ScanResult replay(Path directory, long afterSeq, Visitor visitor) {
        List<Path> segments = JournalFormat.segments(directory);
        Command command = new Command();
        CRC32C crc = new CRC32C();
        long lastSeq = 0;
        long records = 0;
        Path lastSegment = null;
        long endOffset = 0;
        boolean torn = false;

        segments:
        for (int i = 0; i < segments.size(); i++) {
            Path segmentPath = segments.get(i);
            long expectedFirst = JournalFormat.firstSeqOf(segmentPath);
            if (lastSeq != 0 && expectedFirst != lastSeq + 1) {
                torn = true;
                break;
            }
            lastSegment = segmentPath;
            endOffset = 0;
            try (FileChannel channel = FileChannel.open(segmentPath, StandardOpenOption.READ);
                 Arena arena = Arena.ofConfined()) {
                long size = channel.size();
                MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
                byte[] payload = new byte[CommandCodec.LENGTH];
                long offset = 0;
                while (offset + JournalFormat.RECORD_LENGTH <= size) {
                    int length = segment.get(JournalFormat.INT, offset);
                    if (length == 0) {
                        break segments;
                    }
                    if (length == JournalFormat.END_OF_SEGMENT) {
                        continue segments;
                    }
                    if (length != CommandCodec.LENGTH) {
                        torn = true;
                        break segments;
                    }
                    int storedCrc = segment.get(JournalFormat.INT, offset + 4);
                    MemorySegment.copy(segment, java.lang.foreign.ValueLayout.JAVA_BYTE,
                            offset + JournalFormat.HEADER_LENGTH, payload, 0, payload.length);
                    crc.reset();
                    crc.update(payload);
                    if ((int) crc.getValue() != storedCrc) {
                        torn = true;
                        break segments;
                    }
                    long seq = CommandCodec.decode(segment, offset + JournalFormat.HEADER_LENGTH, command);
                    long expected = lastSeq == 0 ? expectedFirst : lastSeq + 1;
                    if (seq != expected) {
                        torn = true;
                        break segments;
                    }
                    lastSeq = seq;
                    offset += JournalFormat.RECORD_LENGTH;
                    endOffset = offset;
                    if (seq > afterSeq) {
                        records++;
                        visitor.onRecord(seq, command);
                    }
                }
                // Ran off the end of a segment without an explicit marker: the writer rolled here.
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return new ScanResult(lastSeq, records, lastSegment, endOffset, torn);
    }
}
