package dev.matchbook.engine.pipeline;

import com.lmax.disruptor.EventHandler;

/**
 * First inbound stage: gives each trading command its journal sequence number and appends it to the
 * journal. The matching stage is gated on this stage's sequence, which the Disruptor advances only
 * after {@link #onEvent} returns for the end of a batch, so nothing is matched before it is journaled
 * (and, under {@link FsyncPolicy#EVERY_BATCH}, flushed).
 */
final class JournalStage implements EventHandler<InboundSlot> {

    private final Journal journal;

    JournalStage(Journal journal) {
        this.journal = journal;
    }

    @Override
    public void onEvent(InboundSlot slot, long sequence, boolean endOfBatch) {
        if (slot.kind == InboundSlot.Kind.COMMAND) {
            slot.journalSeq = journal.append(slot.command);
        } else {
            slot.journalSeq = journal.lastSeq();
        }
        if (endOfBatch) {
            journal.endOfBatch();
        }
    }

    @Override
    public void onShutdown() {
        journal.close();
    }
}
