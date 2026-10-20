package dev.matchbook.engine.pipeline;

import com.lmax.disruptor.EventHandler;
import dev.matchbook.engine.core.EngineSnapshot;
import dev.matchbook.engine.core.MatchingEngine;
import dev.matchbook.engine.core.OrderBook;
import dev.matchbook.engine.core.PriceLevel;
import dev.matchbook.engine.core.Side;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

/**
 * Second inbound stage, the only thread that touches the {@link MatchingEngine}. Trading commands are
 * matched; control slots take snapshots or emit book images at their exact position in the stream.
 */
final class MatchingStage implements EventHandler<InboundSlot> {

    private final MatchingEngine engine;
    private final OutboundSink sink;
    private final SnapshotStore store;
    private final Executor snapshotWriter;
    private final PipelineMetrics metrics;
    private final Recovery recovery;
    private long lastAppliedJournalSeq;

    /** Rebuilds engine state before the first command; runs on the matching thread. Returns the last applied seq. */
    @FunctionalInterface
    interface Recovery {
        long run();
    }

    MatchingStage(MatchingEngine engine, OutboundSink sink, SnapshotStore store, Executor snapshotWriter,
                  PipelineMetrics metrics, Recovery recovery) {
        this.engine = engine;
        this.sink = sink;
        this.store = store;
        this.snapshotWriter = snapshotWriter;
        this.metrics = metrics;
        this.recovery = recovery;
    }

    /**
     * Recovery runs here, on the matching thread, because replayed events are published to the
     * single-producer outbound ring and only this thread may ever publish to it.
     */
    @Override
    public void onStart() {
        lastAppliedJournalSeq = recovery.run();
    }

    @Override
    public void onEvent(InboundSlot slot, long sequence, boolean endOfBatch) {
        switch (slot.kind) {
            case COMMAND -> {
                long start = System.nanoTime();
                sink.beginCommand(slot, start);
                engine.process(slot.command);
                sink.endCommand();
                lastAppliedJournalSeq = slot.journalSeq;
                metrics.commandsMatched++;
                metrics.matchNanosTotal += System.nanoTime() - start;
            }
            case SNAPSHOT -> snapshot(slot.snapshotRequest);
            case BOOK_IMAGE -> bookImage(slot);
        }
    }

    private void snapshot(SnapshotRequest request) {
        long start = System.nanoTime();
        ByteBuffer buffer = ByteBuffer.allocate(EngineSnapshot.maxLength(engine));
        int length = EngineSnapshot.write(engine, buffer, 0);
        byte[] state = new byte[length];
        buffer.get(0, state);
        long pause = System.nanoTime() - start;
        Snapshot snapshot = new Snapshot(lastAppliedJournalSeq, state, pause);
        metrics.snapshots++;
        metrics.lastSnapshotPauseNanos = pause;
        if (request.persist) {
            snapshotWriter.execute(() -> {
                try {
                    store.write(snapshot);
                    request.result.complete(snapshot);
                } catch (RuntimeException e) {
                    request.result.completeExceptionally(e);
                }
            });
        } else {
            request.result.complete(snapshot);
        }
    }

    private void bookImage(InboundSlot slot) {
        OrderBook book = engine.orderBook(slot.command.symbolId());
        if (book == null) {
            return;
        }
        sink.beginCommand(slot, System.nanoTime());
        OutboundEvent start = sink.claim();
        start.type = OutboundEvent.Type.BOOK_IMAGE_START;
        start.symbolId = book.config().symbolId();
        start.symbolSeq = book.symbolSeq();
        start.seq = engine.lastSeq();
        for (Side side : SIDES) {
            for (PriceLevel level = book.bestLevel(side); level != null; level = book.nextLevel(side, level)) {
                OutboundEvent e = sink.claim();
                e.type = OutboundEvent.Type.BOOK_IMAGE_LEVEL;
                e.symbolId = book.config().symbolId();
                e.symbolSeq = book.symbolSeq();
                e.seq = engine.lastSeq();
                e.side = side;
                e.price = level.price();
                e.quantity = level.totalQuantity();
                e.orderCount = level.orderCount();
            }
        }
        OutboundEvent end = sink.claim();
        end.type = OutboundEvent.Type.BOOK_IMAGE_END;
        end.symbolId = book.config().symbolId();
        end.symbolSeq = book.symbolSeq();
        end.seq = engine.lastSeq();
        sink.endCommand();
    }

    private static final Side[] SIDES = {Side.BUY, Side.SELL};
}
