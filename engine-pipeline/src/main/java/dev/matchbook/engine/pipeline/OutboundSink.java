package dev.matchbook.engine.pipeline;

import com.lmax.disruptor.RingBuffer;
import dev.matchbook.engine.core.BookLevelUpdate;
import dev.matchbook.engine.core.EventSink;
import dev.matchbook.engine.core.ExecutionReport;
import dev.matchbook.engine.core.TopOfBook;
import dev.matchbook.engine.core.TradeEvent;

/**
 * Engine {@link EventSink} that copies each event into the outbound ring, tagged with the context of the
 * command being processed. Runs on the matching thread and allocates nothing.
 *
 * <p>Publication lags by one event: an event is published when the next one is claimed, or when the
 * command ends. That lets the last event of each command be flagged {@code lastOfCommand} before any
 * listener can see it, without a separate end-of-command event.
 */
final class OutboundSink implements EventSink {

    private final RingBuffer<OutboundEvent> ring;
    private InboundSlot current;
    private long currentMatchNanos;
    private long pending = -1;

    OutboundSink(RingBuffer<OutboundEvent> ring) {
        this.ring = ring;
    }

    void beginCommand(InboundSlot slot, long matchNanos) {
        this.current = slot;
        this.currentMatchNanos = matchNanos;
    }

    /** Publishes the held-back event, flagged as the command's last. */
    void endCommand() {
        if (pending >= 0) {
            ring.get(pending).lastOfCommand = true;
            ring.publish(pending);
            pending = -1;
        }
        current = null;
    }

    OutboundEvent claim() {
        long sequence = ring.next();
        if (pending >= 0) {
            ring.publish(pending);
        }
        pending = sequence;
        OutboundEvent event = ring.get(sequence);
        if (current != null) {
            event.setContext(current, currentMatchNanos);
        } else {
            event.lastOfCommand = false;
        }
        return event;
    }

    @Override
    public void onExecutionReport(ExecutionReport report) {
        claim().copy(report);
    }

    @Override
    public void onTrade(TradeEvent trade) {
        claim().copy(trade);
    }

    @Override
    public void onBookLevel(BookLevelUpdate update) {
        claim().copy(update);
    }

    @Override
    public void onTopOfBook(TopOfBook top) {
        claim().copy(top);
    }
}
