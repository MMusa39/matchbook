package dev.matchbook.marketdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * One WebSocket client. The feed thread enqueues messages without blocking; a virtual thread writes
 * them to the socket. A client that falls {@value #CAPACITY} messages behind is disconnected rather
 * than allowed to slow the feed for everyone; on reconnect it gets a fresh snapshot.
 */
final class ClientSession {

    private static final Logger LOG = LoggerFactory.getLogger(ClientSession.class);
    static final int CAPACITY = 10_000;

    private final WebSocketSession socket;
    private final BlockingQueue<String> queue = new ArrayBlockingQueue<>(CAPACITY);
    private volatile boolean open = true;

    ClientSession(WebSocketSession socket) {
        this.socket = socket;
        Thread.ofVirtual().name("ws-" + socket.getId()).start(this::drain);
    }

    String id() {
        return socket.getId();
    }

    boolean isOpen() {
        return open;
    }

    /** Never blocks. Returns false if the client was disconnected for being too slow. */
    boolean offer(String json) {
        if (!open) {
            return false;
        }
        if (queue.offer(json)) {
            return true;
        }
        LOG.info("disconnecting slow market data client {}", socket.getId());
        close(CloseStatus.POLICY_VIOLATION.withReason("slow consumer"));
        return false;
    }

    private void drain() {
        try {
            while (open) {
                String message = queue.poll(200, TimeUnit.MILLISECONDS);
                if (message != null && socket.isOpen()) {
                    socket.sendMessage(new TextMessage(message));
                }
            }
        } catch (IOException | IllegalStateException e) {
            close(CloseStatus.SERVER_ERROR);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    void close(CloseStatus status) {
        open = false;
        try {
            if (socket.isOpen()) {
                socket.close(status);
            }
        } catch (IOException ignored) {
            // already gone
        }
    }
}
