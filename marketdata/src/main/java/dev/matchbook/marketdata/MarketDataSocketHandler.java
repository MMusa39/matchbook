package dev.matchbook.marketdata;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocket protocol at {@code /ws/market-data}:
 * <pre>
 * → {"op":"subscribe","symbols":["AAPL","MSFT"]}
 * ← {"type":"snapshot","symbol":"AAPL","seq":41,"bids":[...],"asks":[...],"trades":[...]}
 * ← {"type":"level","symbol":"AAPL","seq":42,...}   {"type":"trade",...}   {"type":"top",...}
 * ← {"type":"stale","symbol":"AAPL"}               (a gap upstream: discard the book, a snapshot follows)
 * → {"op":"unsubscribe","symbols":["AAPL"]}
 * </pre>
 * {@code seq} is the engine's per-symbol sequence: a client that sees anything other than
 * {@code previous + 1} has missed a message and must resubscribe to get a new snapshot.
 */
@Component
public class MarketDataSocketHandler extends TextWebSocketHandler {

    private final MarketDataHub hub;
    private final JsonMapper json;
    private final Map<String, ClientSession> sessions = new ConcurrentHashMap<>();

    public MarketDataSocketHandler(MarketDataHub hub, JsonMapper json) {
        this.hub = hub;
        this.json = json;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession socket) {
        ClientSession session = new ClientSession(socket);
        sessions.put(socket.getId(), session);
        hub.connect(session);
    }

    @Override
    protected void handleTextMessage(WebSocketSession socket, TextMessage message) {
        ClientSession session = sessions.get(socket.getId());
        if (session == null) {
            return;
        }
        JsonNode request;
        try {
            request = json.readTree(message.getPayload());
        } catch (RuntimeException e) {
            session.offer("{\"type\":\"error\",\"message\":\"invalid JSON\"}");
            return;
        }
        String op = request.path("op").asString("");
        for (JsonNode symbol : request.path("symbols")) {
            String name = symbol.asString("");
            switch (op) {
                case "subscribe" -> {
                    if (!hub.subscribe(session, name)) {
                        session.offer(json.writeValueAsString(Map.of("type", "error", "message", "unknown symbol " + name)));
                    }
                }
                case "unsubscribe" -> hub.unsubscribe(session, name);
                default -> session.offer("{\"type\":\"error\",\"message\":\"op must be subscribe or unsubscribe\"}");
            }
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession socket, CloseStatus status) {
        ClientSession session = sessions.remove(socket.getId());
        if (session != null) {
            session.close(status);
            hub.disconnect(session);
        }
    }
}
