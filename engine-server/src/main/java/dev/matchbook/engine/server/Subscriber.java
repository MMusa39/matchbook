package dev.matchbook.engine.server;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * One streaming gRPC client. Engine listener threads {@link #offer} messages into a bounded queue and a
 * virtual thread drains it into the gRPC stream, so a slow client never blocks the engine. A client
 * that falls more than {@code capacity} messages behind is disconnected (RESOURCE_EXHAUSTED); it must
 * reconnect and resynchronise from a fresh image, which is what the market data sequence numbers are for.
 */
final class Subscriber<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Subscriber.class);

    private final String name;
    private final ServerCallStreamObserver<T> observer;
    private final BlockingQueue<T> queue;
    private final Runnable onClose;
    private volatile boolean closed;

    Subscriber(String name, ServerCallStreamObserver<T> observer, int capacity, Runnable onClose) {
        this.name = name;
        this.observer = observer;
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.onClose = onClose;
        observer.setOnCancelHandler(this::close);
        Thread.ofVirtual().name("stream-" + name).start(this::drain);
    }

    /** Called from listener threads. Never blocks. */
    void offer(T message) {
        if (closed) {
            return;
        }
        if (!queue.offer(message)) {
            LOG.warn("subscriber {} is too slow ({} queued); disconnecting", name, queue.size());
            closed = true;
            onClose.run();
            try {
                observer.onError(Status.RESOURCE_EXHAUSTED.withDescription("slow consumer").asRuntimeException());
            } catch (RuntimeException ignored) {
                // stream may already be closed
            }
        }
    }

    boolean isClosed() {
        return closed;
    }

    private void drain() {
        try {
            while (!closed) {
                T message = queue.poll(100, TimeUnit.MILLISECONDS);
                if (message == null) {
                    continue;
                }
                synchronized (observer) {
                    observer.onNext(message);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            LOG.info("subscriber {} stream ended: {}", name, e.toString());
            close();
        }
    }

    void close() {
        if (!closed) {
            closed = true;
            onClose.run();
        }
    }
}
