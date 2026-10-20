package dev.matchbook.marketdata;

import dev.matchbook.proto.v1.BookImage;
import dev.matchbook.proto.v1.BookLevel;
import dev.matchbook.proto.v1.LevelUpdate;
import dev.matchbook.proto.v1.Side;
import dev.matchbook.proto.v1.TopOfBook;
import dev.matchbook.proto.v1.Trade;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Local copy of one symbol's depth, rebuilt from an engine image and kept current by sequenced
 * incrementals. Not thread-safe; {@link MarketDataHub} serialises access per symbol.
 */
public final class BookReplica {

    public record Level(long price, long quantity, int orders) {
    }

    public record TradePrint(long seq, long tradeId, long price, long quantity, String aggressor, long timestamp) {
    }

    /** Outcome of applying an incremental. */
    public enum Applied {
        APPLIED,
        /** Sequence number at or below the current one: already reflected, ignore. */
        STALE,
        /** A sequence number was skipped: the replica can no longer be trusted until a new image. */
        GAP,
        /** No image received yet. */
        NOT_SYNCED
    }

    private static final int RECENT_TRADES = 50;

    private final int symbolId;
    private final TreeMap<Long, Level> bids = new TreeMap<>(Collections.reverseOrder());
    private final TreeMap<Long, Level> asks = new TreeMap<>();
    private final Deque<TradePrint> trades = new ArrayDeque<>();
    private long seq = -1;
    private TopOfBook top;

    public BookReplica(int symbolId) {
        this.symbolId = symbolId;
    }

    public void reset(BookImage image) {
        bids.clear();
        asks.clear();
        for (BookLevel l : image.getBidsList()) {
            bids.put(l.getPrice(), new Level(l.getPrice(), l.getQuantity(), l.getOrderCount()));
        }
        for (BookLevel l : image.getAsksList()) {
            asks.put(l.getPrice(), new Level(l.getPrice(), l.getQuantity(), l.getOrderCount()));
        }
        seq = image.getSymbolSeq();
        top = null;
    }

    public Applied apply(LevelUpdate u) {
        Applied check = check(u.getSymbolSeq());
        if (check != Applied.APPLIED) {
            return check;
        }
        TreeMap<Long, Level> side = u.getSide() == Side.SIDE_BUY ? bids : asks;
        if (u.getQuantity() == 0) {
            side.remove(u.getPrice());
        } else {
            side.put(u.getPrice(), new Level(u.getPrice(), u.getQuantity(), u.getOrderCount()));
        }
        return Applied.APPLIED;
    }

    public Applied apply(Trade t) {
        Applied check = check(t.getSymbolSeq());
        if (check != Applied.APPLIED) {
            return check;
        }
        trades.addFirst(new TradePrint(t.getSymbolSeq(), t.getTradeId(), t.getPrice(), t.getQuantity(),
                t.getAggressorSide() == Side.SIDE_BUY ? "BUY" : "SELL", t.getTimestamp()));
        while (trades.size() > RECENT_TRADES) {
            trades.removeLast();
        }
        return Applied.APPLIED;
    }

    public Applied apply(TopOfBook t) {
        Applied check = check(t.getSymbolSeq());
        if (check == Applied.APPLIED) {
            top = t;
        }
        return check;
    }

    private Applied check(long incoming) {
        if (seq < 0) {
            return Applied.NOT_SYNCED;
        }
        if (incoming <= seq) {
            return Applied.STALE;
        }
        if (incoming != seq + 1) {
            seq = -1;
            return Applied.GAP;
        }
        seq = incoming;
        return Applied.APPLIED;
    }

    public boolean synced() {
        return seq >= 0;
    }

    public long seq() {
        return seq;
    }

    public int symbolId() {
        return symbolId;
    }

    public List<Level> bids(int depth) {
        return firstN(bids, depth);
    }

    public List<Level> asks(int depth) {
        return firstN(asks, depth);
    }

    public List<TradePrint> recentTrades() {
        return List.copyOf(trades);
    }

    public TopOfBook top() {
        return top;
    }

    private static List<Level> firstN(TreeMap<Long, Level> side, int depth) {
        List<Level> out = new ArrayList<>(Math.min(depth, side.size()));
        for (Map.Entry<Long, Level> e : side.entrySet()) {
            if (out.size() == depth) {
                break;
            }
            out.add(e.getValue());
        }
        return out;
    }
}
