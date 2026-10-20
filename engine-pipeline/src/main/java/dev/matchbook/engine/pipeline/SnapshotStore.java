package dev.matchbook.engine.pipeline;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

/**
 * Snapshot files: {@code snapshot-<journalSeq>.snap}, written to a temporary file, forced to disk and
 * atomically renamed, so a crash mid-write never leaves a half snapshot under the real name. Each file
 * carries a CRC; loading falls back to an older snapshot if the newest one is damaged.
 *
 * <p>After a successful write, snapshots beyond {@code keep} are deleted, and so are journal segments
 * that contain only records already covered by the oldest kept snapshot.
 */
final class SnapshotStore {

    private static final System.Logger LOG = System.getLogger(SnapshotStore.class.getName());
    private static final int MAGIC = 0x4D425353; // "MBSS"
    private static final int VERSION = 1;
    private static final int HEADER = 4 + 4 + 8 + 4 + 4;
    private static final String PREFIX = "snapshot-";
    private static final String SUFFIX = ".snap";

    private final Path directory;
    private final int keep;

    SnapshotStore(Path directory, int keep) {
        this.directory = directory;
        this.keep = keep;
    }

    void write(Snapshot snapshot) {
        Path target = directory.resolve(String.format("%s%020d%s", PREFIX, snapshot.journalSeq(), SUFFIX));
        Path temp = directory.resolve(target.getFileName() + ".tmp");
        CRC32C crc = new CRC32C();
        crc.update(snapshot.state());
        ByteBuffer header = ByteBuffer.allocate(HEADER).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(MAGIC).putInt(VERSION).putLong(snapshot.journalSeq())
                .putInt(snapshot.state().length).putInt((int) crc.getValue()).flip();
        try {
            Files.createDirectories(directory);
            try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                ByteBuffer body = ByteBuffer.wrap(snapshot.state());
                while (header.hasRemaining() || body.hasRemaining()) {
                    channel.write(new ByteBuffer[]{header, body});
                }
                channel.force(true);
            }
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        prune();
    }

    /** The newest snapshot that passes validation, if any. */
    Optional<Snapshot> latest() {
        for (Path file : files().reversed()) {
            try {
                Optional<Snapshot> loaded = read(file);
                if (loaded.isPresent()) {
                    return loaded;
                }
            } catch (IOException | RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "ignoring unreadable snapshot " + file, e);
            }
        }
        return Optional.empty();
    }

    private static Optional<Snapshot> read(Path file) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        if (bytes.length < HEADER) {
            return Optional.empty();
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        if (buffer.getInt() != MAGIC || buffer.getInt() != VERSION) {
            return Optional.empty();
        }
        long journalSeq = buffer.getLong();
        int length = buffer.getInt();
        int storedCrc = buffer.getInt();
        if (length != bytes.length - HEADER) {
            return Optional.empty();
        }
        byte[] state = new byte[length];
        buffer.get(state);
        CRC32C crc = new CRC32C();
        crc.update(state);
        if ((int) crc.getValue() != storedCrc) {
            LOG.log(System.Logger.Level.WARNING, "snapshot {0} failed its CRC check", file);
            return Optional.empty();
        }
        return Optional.of(new Snapshot(journalSeq, state, 0));
    }

    private void prune() {
        List<Path> files = files();
        for (int i = 0; i < files.size() - keep; i++) {
            delete(files.get(i));
        }
        List<Path> kept = files.subList(Math.max(0, files.size() - keep), files.size());
        if (kept.isEmpty()) {
            return;
        }
        long oldestKeptSeq = seqOf(kept.getFirst());
        // A segment is fully covered when the next segment starts at or before oldestKeptSeq + 1.
        List<Path> segments = JournalFormat.segments(directory);
        for (int i = 0; i + 1 < segments.size(); i++) {
            if (JournalFormat.firstSeqOf(segments.get(i + 1)) <= oldestKeptSeq + 1) {
                delete(segments.get(i));
            } else {
                break;
            }
        }
    }

    private List<Path> files() {
        if (!Files.isDirectory(directory)) {
            return List.of();
        }
        try (Stream<Path> list = Files.list(directory)) {
            return list.filter(p -> p.getFileName().toString().startsWith(PREFIX)
                            && p.getFileName().toString().endsWith(SUFFIX))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long seqOf(Path file) {
        String name = file.getFileName().toString();
        return Long.parseLong(name.substring(PREFIX.length(), name.length() - SUFFIX.length()));
    }

    private static void delete(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            LOG.log(System.Logger.Level.WARNING, "could not delete " + file, e);
        }
    }
}
