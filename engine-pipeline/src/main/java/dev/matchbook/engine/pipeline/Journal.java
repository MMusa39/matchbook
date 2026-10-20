package dev.matchbook.engine.pipeline;

import dev.matchbook.engine.core.Command;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

/**
 * Append-only, memory-mapped command journal (single writer: the journal stage thread).
 *
 * <p>Appending writes straight into a mapped {@link MemorySegment}: no system call, no copy through a
 * user-space buffer and no allocation. With {@link FsyncPolicy#EVERY_BATCH} the range written since the
 * last flush is forced to the device at the end of each Disruptor batch, which amortises one flush over
 * every command that arrived together (group commit).
 *
 * <p>Uses the Java FFM API ({@code FileChannel.map(..., Arena)}) rather than {@code MappedByteBuffer}
 * or {@code Unsafe}: the mapping is released deterministically when the arena closes instead of
 * whenever the GC collects a buffer, and no {@code --add-opens} or {@code --add-exports} flags are
 * needed. Segments are limited to 2 GiB because the CRC is computed through a {@link ByteBuffer} view.
 *
 * <p>The next segment is created, mapped and (with pretouch) pre-faulted by a background thread while
 * the current one fills, so a roll on the journal thread is a rename and a pointer swap. Rolling
 * synchronously used to stall the journal thread for 100 ms+ on a 1 GiB segment, which back-pressured
 * the whole pipeline (RESULTS §3). See ADR-0005.
 */
public final class Journal implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Journal.class.getName());

    private final Path directory;
    private final long segmentBytes;
    private final FsyncPolicy fsync;
    private final CRC32C crc = new CRC32C();

    private FileChannel channel;
    private Arena arena;
    private MemorySegment segment;
    private ByteBuffer crcView;
    private long position;
    private long syncedPosition;
    private long lastSeq;
    private long flushes;
    private volatile boolean pretouch;

    /** A mapped segment file prepared ahead of time under a temporary name. */
    private record Mapping(Path path, FileChannel channel, Arena arena, MemorySegment segment) {
    }

    private static final String NEXT_PREFIX = "journal-next-";
    private ExecutorService preparer;
    private CompletableFuture<Mapping> next;
    private int prepared;

    private Journal(Path directory, long segmentBytes, FsyncPolicy fsync) {
        this.directory = directory;
        this.segmentBytes = segmentBytes;
        this.fsync = fsync;
    }

    /**
     * Pre-faults every page of each segment when it is mapped, so appends never take a page fault.
     * Without it, the first write to each fresh page (16 KiB on Apple Silicon, 4 KiB on x86 Linux) faults
     * into the kernel to allocate a zeroed page, on the journal thread, in the middle of a command. The cost
     * moves to segment creation, which is why segments should be sized so that rolls are rare.
     */
    public Journal pretouch(boolean enabled) {
        this.pretouch = enabled;
        if (enabled) {
            touchFrom(position);
        }
        return this;
    }

    private void touchFrom(long from) {
        touch(segment, from);
    }

    private static void touch(MemorySegment segment, long from) {
        long page = 4096;
        for (long offset = (from + page - 1) / page * page; offset < segment.byteSize(); offset += page) {
            // Re-writing the existing byte (zero in a fresh segment) faults the page in without changing data.
            segment.set(java.lang.foreign.ValueLayout.JAVA_BYTE, offset,
                    segment.get(java.lang.foreign.ValueLayout.JAVA_BYTE, offset));
        }
    }

    /** Opens the journal for appending, as {@link #open(Path, long, FsyncPolicy, long)} with no snapshot. */
    public static Journal open(Path directory, long segmentBytes, FsyncPolicy fsync) {
        return open(directory, segmentBytes, fsync, 0);
    }

    /**
     * Opens the journal in {@code directory} for appending after its last valid record, creating it if
     * needed. A damaged tail (torn record) is discarded: appending resumes over it.
     *
     * <p>If the journal ends before {@code snapshotSeq} (possible after power loss with
     * {@link FsyncPolicy#NONE}: the snapshot was forced to disk, the latest journal pages were not), the
     * existing segments are moved aside with a {@code .stale} suffix and a new journal starts at
     * {@code snapshotSeq + 1}, so sequence numbers are never reused.
     */
    public static Journal open(Path directory, long segmentBytes, FsyncPolicy fsync, long snapshotSeq) {
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Journal journal = new Journal(directory, segmentBytes, fsync);
        deleteLeftoverNextSegments(directory);
        JournalReader.ScanResult scan = JournalReader.scanTail(directory);
        if (scan.torn()) {
            LOG.log(System.Logger.Level.WARNING, "journal tail damaged after seq {0}; truncating there", scan.lastSeq());
        }
        if (scan.lastSeq() < snapshotSeq) {
            LOG.log(System.Logger.Level.WARNING, "journal ends at {0}, before snapshot {1}; starting a new journal",
                    scan.lastSeq(), snapshotSeq);
            for (Path segment : JournalFormat.segments(directory)) {
                try {
                    Files.move(segment, segment.resolveSibling(segment.getFileName() + ".stale"));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            journal.lastSeq = snapshotSeq;
            journal.map(JournalFormat.segmentPath(directory, snapshotSeq + 1), 0);
            return journal;
        }
        journal.lastSeq = scan.lastSeq();
        if (scan.lastSegment() == null) {
            journal.map(JournalFormat.segmentPath(directory, 1), 0);
        } else if (scan.endOffset() + JournalFormat.RECORD_LENGTH + Integer.BYTES > segmentBytesOf(scan.lastSegment(), segmentBytes)) {
            journal.map(scan.lastSegment(), scan.endOffset());
            journal.roll();
        } else {
            journal.map(scan.lastSegment(), scan.endOffset());
            // Clear a torn record's header so a crash before the next append cannot resurrect it.
            journal.segment.set(JournalFormat.INT, journal.position, 0);
            journal.segment.set(JournalFormat.INT, journal.position + 4, 0);
        }
        return journal;
    }

    private static void deleteLeftoverNextSegments(Path directory) {
        try (Stream<Path> files = Files.list(directory)) {
            for (Path p : files.filter(f -> f.getFileName().toString().startsWith(NEXT_PREFIX)).toList()) {
                Files.deleteIfExists(p);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long segmentBytesOf(Path segment, long configured) {
        try {
            return Math.max(Files.size(segment), configured);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Appends a command, returning the journal sequence number it was given. */
    public long append(Command command) {
        if (position + JournalFormat.RECORD_LENGTH + Integer.BYTES > segment.byteSize()) {
            roll();
        }
        if (next == null) {
            prepareNext();
        }
        long seq = ++lastSeq;
        long payloadOffset = position + JournalFormat.HEADER_LENGTH;
        CommandCodec.encode(command, seq, segment, payloadOffset);
        crcView.limit((int) (payloadOffset + CommandCodec.LENGTH)).position((int) payloadOffset);
        crc.reset();
        crc.update(crcView);
        segment.set(JournalFormat.INT, position + 4, (int) crc.getValue());
        // Length last: until it is written the record does not exist for a reader.
        segment.set(JournalFormat.INT, position, CommandCodec.LENGTH);
        position += JournalFormat.RECORD_LENGTH;
        return seq;
    }

    /** Called at the end of each batch; forces the newly written range to disk under {@link FsyncPolicy#EVERY_BATCH}. */
    public void endOfBatch() {
        if (fsync == FsyncPolicy.EVERY_BATCH && position > syncedPosition) {
            segment.asSlice(syncedPosition, position - syncedPosition).force();
            syncedPosition = position;
            flushes++;
        }
    }

    public long lastSeq() {
        return lastSeq;
    }

    public long flushes() {
        return flushes;
    }

    private void roll() {
        if (position + Integer.BYTES <= segment.byteSize()) {
            segment.set(JournalFormat.INT, position, JournalFormat.END_OF_SEGMENT);
        }
        if (fsync == FsyncPolicy.EVERY_BATCH) {
            segment.force();
        }
        Path target = JournalFormat.segmentPath(directory, lastSeq + 1);
        Mapping ready = takeNext();
        if (ready == null) {
            unmap();
            map(target, 0);
            if (pretouch) {
                touchFrom(0);
            }
            return;
        }
        try {
            // The mapping stays valid across the rename; only the name changes.
            Files.move(ready.path(), target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        // Unmapping a large resident segment (munmap of every page) is slow too: do it off this thread.
        FileChannel oldChannel = channel;
        Arena oldArena = arena;
        preparer.execute(() -> {
            try {
                oldArena.close();
                oldChannel.close();
            } catch (IOException | RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "closing a full journal segment failed", e);
            }
        });
        channel = ready.channel();
        arena = ready.arena();
        segment = ready.segment();
        crcView = segment.asByteBuffer();
        position = 0;
        syncedPosition = 0;
        prepareNext();
    }

    /** Starts creating the next segment in the background. */
    private void prepareNext() {
        if (preparer == null) {
            preparer = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "journal-preparer");
                t.setDaemon(true);
                return t;
            });
        }
        Path path = directory.resolve(NEXT_PREFIX + (prepared++) + ".tmp");
        boolean touch = pretouch;
        next = CompletableFuture.supplyAsync(() -> {
            try {
                FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ,
                        StandardOpenOption.WRITE);
                Arena a = Arena.ofShared();
                MemorySegment seg = ch.map(FileChannel.MapMode.READ_WRITE, 0, segmentBytes, a);
                if (touch) {
                    touch(seg, 0);
                }
                return new Mapping(path, ch, a, seg);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, preparer);
    }

    /** The prepared next segment, waiting for it if necessary; {@code null} if preparing failed. */
    private Mapping takeNext() {
        CompletableFuture<Mapping> future = next;
        next = null;
        if (future == null) {
            return null;
        }
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            LOG.log(System.Logger.Level.WARNING, "preparing the next journal segment failed; mapping inline",
                    e.getCause());
            return null;
        }
    }

    private void map(Path path, long offset) {
        try {
            channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            long size = Math.max(channel.size(), segmentBytes);
            if (size > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("journal segments must be smaller than 2 GiB");
            }
            arena = Arena.ofShared();
            segment = channel.map(FileChannel.MapMode.READ_WRITE, 0, size, arena);
            crcView = segment.asByteBuffer();
            position = offset;
            syncedPosition = offset;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void unmap() {
        try {
            arena.close();
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Flushes (under {@link FsyncPolicy#EVERY_BATCH}) and unmaps. Records already written stay valid. */
    @Override
    public void close() {
        endOfBatch();
        unmap();
        Mapping unused = takeNext();
        if (unused != null) {
            try {
                unused.arena().close();
                unused.channel().close();
                Files.deleteIfExists(unused.path());
            } catch (IOException e) {
                LOG.log(System.Logger.Level.WARNING, "could not remove prepared segment " + unused.path(), e);
            }
        }
        if (preparer != null) {
            preparer.shutdownNow();
        }
    }
}
