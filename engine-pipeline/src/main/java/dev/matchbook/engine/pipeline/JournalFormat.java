package dev.matchbook.engine.pipeline;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * On-disk layout shared by {@link Journal} and {@link JournalReader}.
 *
 * <p>A journal is a sequence of segment files named {@code journal-<firstSeq>.log} (20-digit,
 * zero-padded, so lexical order is sequence order). Each segment is pre-sized and zero-filled, and
 * holds back-to-back 80-byte records:
 * <pre>
 * int length   payload length (72) once the record is complete; 0 = end of journal; -1 = end of segment
 * int crc32c   CRC32C of the payload
 * byte[72]     {@link CommandCodec} payload, which starts with the record's journal sequence number
 * </pre>
 * The length is written last, so a record torn by a process crash reads as "end of journal". A record
 * torn by an OS crash or power loss (pages persisted out of order) fails its CRC, and replay stops there.
 */
final class JournalFormat {

    static final int HEADER_LENGTH = 8;
    static final int RECORD_LENGTH = HEADER_LENGTH + CommandCodec.LENGTH;
    static final int END_OF_SEGMENT = -1;
    static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private static final String PREFIX = "journal-";
    private static final String SUFFIX = ".log";

    private JournalFormat() {
    }

    static Path segmentPath(Path directory, long firstSeq) {
        return directory.resolve(String.format("%s%020d%s", PREFIX, firstSeq, SUFFIX));
    }

    static long firstSeqOf(Path segment) {
        String name = segment.getFileName().toString();
        return Long.parseLong(name.substring(PREFIX.length(), name.length() - SUFFIX.length()));
    }

    /** Segment files in sequence order. */
    static List<Path> segments(Path directory) {
        if (!Files.isDirectory(directory)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(p -> {
                        String n = p.getFileName().toString();
                        return n.startsWith(PREFIX) && n.endsWith(SUFFIX);
                    })
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
