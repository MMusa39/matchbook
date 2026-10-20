package dev.matchbook.marketdata;

import dev.matchbook.proto.v1.BookImage;
import dev.matchbook.proto.v1.LevelUpdate;
import dev.matchbook.proto.v1.Side;
import dev.matchbook.proto.v1.TopOfBook;
import dev.matchbook.proto.v1.Trade;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Book replicas and their subscribers. For each symbol, applying an update and fanning it out happen
 * under the replica's lock, and so do taking a snapshot and registering a new subscriber. A new
 * subscriber therefore receives a snapshot at sequence {@code n} followed by exactly the updates
 * {@code n+1, n+2, ...}: no gap and no duplicate between snapshot and stream.
 */
@Component
public class MarketDataHub {

    public static final int SNAPSHOT_DEPTH = 50;

    private final Map<Integer, SymbolInfo> symbolsById;
    private final Map<String, SymbolInfo> symbolsByName;
    private final Map<Integer, BookReplica> replicas = new ConcurrentHashMap<>();
    private final Map<Integer, Set<ClientSession>> subscribers = new ConcurrentHashMap<>();
    private final JsonMapper json;
    private final Counter messages;
    private final Counter gaps;
    private final Set<ClientSession> sessions = ConcurrentHashMap.newKeySet();
    private Runnable resync = () -> { };

    public MarketDataHub(SymbolCatalog catalog, JsonMapper json, MeterRegistry meters) {
        this.symbolsById = catalog.byId();
        this.symbolsByName = catalog.byName();
        this.json = json;
        for (SymbolInfo s : symbolsById.values()) {
            replicas.put(s.id(), new BookReplica(s.id()));
            subscribers.put(s.id(), ConcurrentHashMap.newKeySet());
            Gauge.builder("matchbook.marketdata.symbol.seq", replicas.get(s.id()), BookReplica::seq)
                    .tag("symbol", s.symbol()).register(meters);
        }
        this.messages = Counter.builder("matchbook.marketdata.messages").register(meters);
        this.gaps = Counter.builder("matchbook.marketdata.gaps").register(meters);
        Gauge.builder("matchbook.marketdata.sessions", sessions, Set::size).register(meters);
    }

    /** Called when the replica detects a gap: the feed must reconnect to get fresh images. */
    void onResyncNeeded(Runnable resync) {
        this.resync = resync;
    }

    // ------------------------------------------------------------------ feed side

    void onImage(BookImage image) {
        BookReplica replica = replicas.get(image.getSymbolId());
        if (replica == null) {
            return;
        }
        synchronized (replica) {
            replica.reset(image);
            String snapshot = snapshotJson(replica);
            for (ClientSession s : subscribers.get(replica.symbolId())) {
                send(s, snapshot);
            }
        }
    }

    void onLevel(LevelUpdate u) {
        apply(u.getSymbolId(), replica -> replica.apply(u), symbol -> message("level", symbol, u.getSymbolSeq(),
                "side", u.getSide() == Side.SIDE_BUY ? "BUY" : "SELL", "price", symbol.formatPrice(u.getPrice()),
                "quantity", u.getQuantity(), "orders", u.getOrderCount()));
    }

    void onTrade(Trade t) {
        apply(t.getSymbolId(), replica -> replica.apply(t), symbol -> message("trade", symbol, t.getSymbolSeq(),
                "tradeId", t.getTradeId(), "price", symbol.formatPrice(t.getPrice()), "quantity", t.getQuantity(),
                "aggressor", t.getAggressorSide() == Side.SIDE_BUY ? "BUY" : "SELL", "timestamp", t.getTimestamp()));
    }

    void onTop(TopOfBook t) {
        apply(t.getSymbolId(), replica -> replica.apply(t), symbol -> message("top", symbol, t.getSymbolSeq(),
                "bid", t.getBidQuantity() == 0 ? null : symbol.formatPrice(t.getBidPrice()),
                "bidQuantity", t.getBidQuantity(),
                "ask", t.getAskQuantity() == 0 ? null : symbol.formatPrice(t.getAskPrice()),
                "askQuantity", t.getAskQuantity()));
    }

    private void apply(int symbolId, java.util.function.Function<BookReplica, BookReplica.Applied> update,
                       java.util.function.Function<SymbolInfo, Map<String, Object>> render) {
        BookReplica replica = replicas.get(symbolId);
        if (replica == null) {
            return;
        }
        boolean gap = false;
        synchronized (replica) {
            switch (update.apply(replica)) {
                case APPLIED -> {
                    String text = json.writeValueAsString(render.apply(symbolsById.get(symbolId)));
                    for (ClientSession s : subscribers.get(symbolId)) {
                        send(s, text);
                    }
                }
                case GAP -> {
                    gap = true;
                    String stale = json.writeValueAsString(Map.of("type", "stale",
                            "symbol", symbolsById.get(symbolId).symbol()));
                    for (ClientSession s : subscribers.get(symbolId)) {
                        send(s, stale);
                    }
                }
                case STALE, NOT_SYNCED -> {
                }
            }
        }
        if (gap) {
            gaps.increment();
            resync.run();
        }
    }

    // ------------------------------------------------------------------ client side

    void connect(ClientSession session) {
        sessions.add(session);
    }

    void disconnect(ClientSession session) {
        sessions.remove(session);
        subscribers.values().forEach(set -> set.remove(session));
    }

    /** Sends the current snapshot and registers for incrementals, atomically with respect to the feed. */
    boolean subscribe(ClientSession session, String symbolName) {
        SymbolInfo symbol = symbolsByName.get(symbolName);
        if (symbol == null) {
            return false;
        }
        BookReplica replica = replicas.get(symbol.id());
        synchronized (replica) {
            if (replica.synced()) {
                send(session, snapshotJson(replica));
            }
            // If not synced yet, the snapshot arrives with the first image.
            subscribers.get(symbol.id()).add(session);
        }
        return true;
    }

    void unsubscribe(ClientSession session, String symbolName) {
        SymbolInfo symbol = symbolsByName.get(symbolName);
        if (symbol != null) {
            subscribers.get(symbol.id()).remove(session);
        }
    }

    /** Snapshot as a map, for the REST endpoint. */
    public Map<String, Object> snapshot(String symbolName) {
        SymbolInfo symbol = symbolsByName.get(symbolName);
        if (symbol == null) {
            return null;
        }
        BookReplica replica = replicas.get(symbol.id());
        synchronized (replica) {
            return snapshotMap(replica);
        }
    }

    private String snapshotJson(BookReplica replica) {
        return json.writeValueAsString(snapshotMap(replica));
    }

    private Map<String, Object> snapshotMap(BookReplica replica) {
        SymbolInfo symbol = symbolsById.get(replica.symbolId());
        Map<String, Object> m = message("snapshot", symbol, replica.seq(),
                "bids", levels(symbol, replica.bids(SNAPSHOT_DEPTH)),
                "asks", levels(symbol, replica.asks(SNAPSHOT_DEPTH)),
                "trades", replica.recentTrades().stream().map(t -> Map.<String, Object>of(
                        "seq", t.seq(), "tradeId", t.tradeId(), "price", symbol.formatPrice(t.price()),
                        "quantity", t.quantity(), "aggressor", t.aggressor(), "timestamp", t.timestamp())).toList());
        m.put("synced", replica.synced());
        return m;
    }

    private static List<Map<String, Object>> levels(SymbolInfo symbol, List<BookReplica.Level> levels) {
        return levels.stream().map(l -> Map.<String, Object>of("price", symbol.formatPrice(l.price()),
                "quantity", l.quantity(), "orders", l.orders())).toList();
    }

    private static Map<String, Object> message(String type, SymbolInfo symbol, long seq, Object... fields) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", type);
        m.put("symbol", symbol.symbol());
        m.put("seq", seq);
        for (int i = 0; i < fields.length; i += 2) {
            m.put((String) fields[i], fields[i + 1]);
        }
        return m;
    }

    private void send(ClientSession session, String text) {
        if (session.offer(text)) {
            messages.increment();
        } else {
            disconnect(session);
        }
    }
}
