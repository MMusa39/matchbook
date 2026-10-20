package dev.matchbook.engine.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Copies outbound events into strings, thread-safely, for assertions. */
final class RecordingListener implements OutboundListener {

    record Seen(String text, long journalSeq, long correlationId, boolean lastOfCommand) {
    }

    private final List<Seen> seen = new ArrayList<>();

    @Override
    public synchronized void onEvent(OutboundEvent event, boolean endOfBatch) {
        seen.add(new Seen(event.describe(), event.journalSeq(), event.correlationId(), event.lastOfCommand()));
        notifyAll();
    }

    synchronized List<Seen> awaitAtLeast(int count, long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (seen.size() < count) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new AssertionError("timed out with " + seen.size() + " of " + count + " events");
            }
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
        return List.copyOf(seen);
    }

    /** Waits until an event whose text starts with {@code prefix} has been seen. */
    synchronized List<Seen> awaitText(String prefix, long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (seen.stream().noneMatch(s -> s.text().startsWith(prefix))) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new AssertionError("timed out waiting for " + prefix);
            }
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
        return List.copyOf(seen);
    }

    synchronized List<Seen> snapshot() {
        return List.copyOf(seen);
    }
}
