package dev.matchbook.engine.pipeline;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import dev.matchbook.engine.core.Command;
import dev.matchbook.engine.core.EngineSnapshot;
import dev.matchbook.engine.core.MatchingEngine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * One engine shard: inbound ring → journal stage → matching stage → outbound ring → listeners.
 *
 * <pre>
 *  producers ──▶ [inbound ring] ──▶ JournalStage ──▶ MatchingStage ──▶ [outbound ring] ──▶ listener 1..n
 *  (gRPC, load      multi-producer      mmap append      engine.process      single producer     (parallel,
 *   generator)                          + group fsync                                             independent)
 * </pre>
 *
 * <p>{@link #start} recovers before accepting input: it loads the newest valid snapshot, replays the
 * journal records after it through the engine (their events go to the listeners again, marked with
 * correlation id 0; consumers de-duplicate by event sequence), and only then starts the stages.
 */
public final class EnginePipeline implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(EnginePipeline.class.getName());

    /** What startup recovery found and did. */
    public record RecoveryInfo(long snapshotJournalSeq, long replayedCommands, long lastJournalSeq,
                               boolean journalTailWasDamaged, long recoveryNanos) {
    }

    private final PipelineConfig config;
    private final Disruptor<InboundSlot> inbound;
    private final Disruptor<OutboundEvent> outbound;
    private final RingBuffer<InboundSlot> inboundRing;
    private final RingBuffer<OutboundEvent> outboundRing;
    private final MatchingEngine engine;
    private final PipelineMetrics metrics = new PipelineMetrics();
    private final ExecutorService snapshotWriter;
    private final ScheduledExecutorService scheduler;
    private final RecoveryInfo recoveryInfo;
    private final Journal journal;
    private volatile boolean running;

    private EnginePipeline(PipelineConfig config, List<OutboundListener> listeners) {
        this.config = config;
        try {
            Files.createDirectories(config.dataDirectory());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        int shard = config.engine().shardId();

        List<String> listenerNames = new ArrayList<>();
        for (int i = 0; i < listeners.size(); i++) {
            listenerNames.add("outbound-" + shard + "-" + listeners.get(i).getClass().getSimpleName());
        }
        this.outbound = new Disruptor<>(OutboundEvent::new, config.outboundRingSize(),
                new PinnedThreadFactory(listenerNames, List.of()), ProducerType.SINGLE,
                config.outboundWait().create());
        this.outbound.setDefaultExceptionHandler(new LogAndContinue<>());
        if (!listeners.isEmpty()) {
            @SuppressWarnings("unchecked")
            EventHandler<OutboundEvent>[] handlers = listeners.stream().map(ListenerAdapter::new)
                    .toArray(EventHandler[]::new);
            this.outbound.handleEventsWith(handlers);
        }
        this.outboundRing = outbound.start();

        OutboundSink sink = new OutboundSink(outboundRing);
        this.engine = new MatchingEngine(config.engine(), sink);
        SnapshotStore store = new SnapshotStore(config.dataDirectory(), config.snapshotsToKeep());

        long snapshotSeqForJournal = store.latest().map(Snapshot::journalSeq).orElse(0L);
        this.journal = Journal.open(config.dataDirectory(), config.journalSegmentBytes(), config.fsync(),
                snapshotSeqForJournal).pretouch(config.pretouchJournal());
        // Where the journal ends now; nothing can be appended until start() returns.
        long journalEnd = journal.lastSeq();
        CompletableFuture<RecoveryInfo> recovered = new CompletableFuture<>();
        MatchingStage.Recovery recovery = () -> {
            try {
                RecoveryInfo info = recover(store, sink, journalEnd);
                recovered.complete(info);
                return info.lastJournalSeq();
            } catch (RuntimeException | Error e) {
                recovered.completeExceptionally(e);
                throw e;
            }
        };

        this.snapshotWriter = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "snapshot-writer-" + shard);
            t.setDaemon(true);
            return t;
        });
        this.inbound = new Disruptor<>(InboundSlot::new, config.inboundRingSize(),
                new PinnedThreadFactory(List.of("journal-" + shard, "matching-" + shard),
                        List.of(config.journalCpu(), config.matchingCpu())),
                config.multiProducer() ? ProducerType.MULTI : ProducerType.SINGLE, config.inboundWait().create());
        this.inbound.setDefaultExceptionHandler(new Fatal<>());
        this.inbound.handleEventsWith(new JournalStage(journal))
                .then(new MatchingStage(engine, sink, store, snapshotWriter, metrics, recovery));
        this.inboundRing = inbound.start();
        try {
            this.recoveryInfo = recovered.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            inbound.halt();
            outbound.halt();
            throw new IllegalStateException("interrupted during recovery", e);
        } catch (java.util.concurrent.ExecutionException e) {
            inbound.halt();
            outbound.halt();
            throw new IllegalStateException("recovery failed", e.getCause());
        }
        if (recoveryInfo.snapshotJournalSeq() > 0 || recoveryInfo.replayedCommands() > 0) {
            LOG.log(System.Logger.Level.INFO, "shard {0} recovered: snapshot at {1}, replayed {2} commands in {3} ms",
                    shard, recoveryInfo.snapshotJournalSeq(), recoveryInfo.replayedCommands(),
                    recoveryInfo.recoveryNanos() / 1_000_000);
        }
        this.running = true;

        if (config.snapshotInterval().isPositive()) {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "snapshot-timer-" + shard);
                t.setDaemon(true);
                return t;
            });
            long millis = config.snapshotInterval().toMillis();
            scheduler.scheduleAtFixedRate(() -> snapshot(true), millis, millis, TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
        }
    }

    /**
     * Loads the newest valid snapshot and replays the journal records after it, up to {@code journalEnd}.
     * Runs on the matching thread before the first command.
     */
    private RecoveryInfo recover(SnapshotStore store, OutboundSink sink, long journalEnd) {
        long start = System.nanoTime();
        long snapshotSeq = 0;
        Optional<Snapshot> snapshot = store.latest();
        if (snapshot.isPresent()) {
            byte[] state = snapshot.get().state();
            EngineSnapshot.read(engine, ByteBuffer.wrap(state), 0, state.length);
            snapshotSeq = snapshot.get().journalSeq();
        }
        InboundSlot replaySlot = new InboundSlot();
        long[] replayed = {0};
        JournalReader.ScanResult scan = JournalReader.replay(config.dataDirectory(), snapshotSeq, (seq, command) -> {
            if (seq > journalEnd) {
                return;
            }
            replaySlot.journalSeq = seq;
            sink.beginCommand(replaySlot, 0);
            engine.process(command);
            sink.endCommand();
            replayed[0]++;
        });
        long last = Math.max(snapshotSeq, Math.min(scan.lastSeq(), journalEnd));
        if (last < journalEnd) {
            // Damage in the middle of the journal: the engine would silently miss commands that the
            // journal still holds and would append after. Refuse to start; an operator must intervene.
            throw new IllegalStateException("journal is damaged at seq " + (last + 1)
                    + " but holds records up to " + journalEnd);
        }
        return new RecoveryInfo(snapshotSeq, replayed[0], last, scan.torn(), System.nanoTime() - start);
    }

    /** Recovers from the data directory, then starts the pipeline. */
    public static EnginePipeline start(PipelineConfig config, List<OutboundListener> listeners) {
        return new EnginePipeline(config, List.copyOf(listeners));
    }

    // ------------------------------------------------------------------ input

    /** Publishes a command, waiting for ring space if necessary. */
    public void publish(Command command, long correlationId, long ingressNanos) {
        publish(command, correlationId, ingressNanos, 0, 0, 0, (byte) 0);
    }

    /** Publishes a command with the caller's trace context, waiting for ring space if necessary. */
    public void publish(Command command, long correlationId, long ingressNanos,
                        long traceIdHigh, long traceIdLow, long spanId, byte traceFlags) {
        long sequence = inboundRing.next();
        fill(inboundRing.get(sequence), command, correlationId, ingressNanos, traceIdHigh, traceIdLow, spanId,
                traceFlags);
        inboundRing.publish(sequence);
    }

    /** Publishes a command if the ring has space; returns {@code false} instead of waiting when it is full. */
    public boolean tryPublish(Command command, long correlationId, long ingressNanos,
                              long traceIdHigh, long traceIdLow, long spanId, byte traceFlags) {
        long sequence;
        try {
            sequence = inboundRing.tryNext();
        } catch (InsufficientCapacityException e) {
            return false;
        }
        fill(inboundRing.get(sequence), command, correlationId, ingressNanos, traceIdHigh, traceIdLow, spanId,
                traceFlags);
        inboundRing.publish(sequence);
        return true;
    }

    private static void fill(InboundSlot slot, Command command, long correlationId, long ingressNanos,
                             long traceIdHigh, long traceIdLow, long spanId, byte traceFlags) {
        slot.clearMetadata();
        slot.command.copyFrom(command);
        slot.correlationId = correlationId;
        slot.ingressNanos = ingressNanos;
        slot.traceIdHigh = traceIdHigh;
        slot.traceIdLow = traceIdLow;
        slot.spanId = spanId;
        slot.traceFlags = traceFlags;
    }

    /**
     * Captures engine state at the current point in the stream. With {@code persist} the snapshot is
     * also written to disk (off the matching thread) before the future completes.
     */
    public CompletableFuture<Snapshot> snapshot(boolean persist) {
        SnapshotRequest request = new SnapshotRequest(persist);
        long sequence = inboundRing.next();
        InboundSlot slot = inboundRing.get(sequence);
        slot.clearMetadata();
        slot.kind = InboundSlot.Kind.SNAPSHOT;
        slot.snapshotRequest = request;
        inboundRing.publish(sequence);
        return request.result;
    }

    /** Asks the matching thread to emit a full depth image of a symbol, consistent with the event stream. */
    public void requestBookImage(int symbolId) {
        long sequence = inboundRing.next();
        InboundSlot slot = inboundRing.get(sequence);
        slot.clearMetadata();
        slot.kind = InboundSlot.Kind.BOOK_IMAGE;
        slot.command.cancel(0, 0, 0, symbolId, 0);
        inboundRing.publish(sequence);
    }

    // ------------------------------------------------------------------ monitoring

    public RecoveryInfo recoveryInfo() {
        return recoveryInfo;
    }

    public PipelineMetrics metrics() {
        return metrics;
    }

    /** Commands published but not yet matched. */
    public long inboundDepth() {
        return inboundRing.getCursor() - inboundRing.getMinimumGatingSequence();
    }

    /** Events emitted but not yet consumed by the slowest listener. */
    public long outboundDepth() {
        return outboundRing.getCursor() - outboundRing.getMinimumGatingSequence();
    }

    public long lastJournalSeq() {
        return journal.lastSeq();
    }

    public long journalFlushes() {
        return journal.flushes();
    }

    public PipelineConfig config() {
        return config;
    }

    public boolean isRunning() {
        return running;
    }

    // ------------------------------------------------------------------ lifecycle

    /**
     * Clean shutdown: stops the snapshot timer, writes a final snapshot so the next start replays
     * nothing, drains the inbound ring (closing the journal), then drains the outbound ring.
     */
    @Override
    public void close() {
        if (!running) {
            return;
        }
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        try {
            snapshot(true).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING, "final snapshot failed", e);
        }
        inbound.shutdown();
        outbound.shutdown();
        snapshotWriter.shutdown();
        try {
            snapshotWriter.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stops every thread immediately, without a final snapshot and without draining the rings: the
     * on-disk state is whatever a crash at this instant would leave. For tests and failure drills.
     */
    public void halt() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        snapshotWriter.shutdownNow();
        inbound.halt();
        outbound.halt();
    }

    // ------------------------------------------------------------------ plumbing

    private static final class ListenerAdapter implements EventHandler<OutboundEvent> {
        private final OutboundListener listener;

        ListenerAdapter(OutboundListener listener) {
            this.listener = listener;
        }

        @Override
        public void onEvent(OutboundEvent event, long sequence, boolean endOfBatch) throws Exception {
            listener.onEvent(event, endOfBatch);
        }

        @Override
        public void onStart() {
            listener.onStart();
        }

        @Override
        public void onShutdown() {
            listener.onShutdown();
        }
    }

    /** A failing inbound stage would corrupt determinism if it carried on: log and stop that stage. */
    private static final class Fatal<T> implements ExceptionHandler<T> {
        @Override
        public void handleEventException(Throwable ex, long sequence, T event) {
            LOG.log(System.Logger.Level.ERROR, "fatal error in engine stage at sequence " + sequence, ex);
            throw new IllegalStateException("engine stage failed", ex);
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            LOG.log(System.Logger.Level.ERROR, "engine stage failed to start", ex);
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            LOG.log(System.Logger.Level.ERROR, "engine stage failed to shut down", ex);
        }
    }

    /** A failing listener must not stop matching: it logs and skips the event; listeners own retries. */
    private static final class LogAndContinue<T> implements ExceptionHandler<T> {
        @Override
        public void handleEventException(Throwable ex, long sequence, T event) {
            LOG.log(System.Logger.Level.ERROR, "listener failed at outbound sequence " + sequence, ex);
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            LOG.log(System.Logger.Level.ERROR, "listener failed to start", ex);
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            LOG.log(System.Logger.Level.ERROR, "listener failed to shut down", ex);
        }
    }
}
