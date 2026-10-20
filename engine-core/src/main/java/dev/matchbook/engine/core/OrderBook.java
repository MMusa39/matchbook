package dev.matchbook.engine.core;

import java.util.BitSet;

/**
 * The resting orders for one symbol. A pure data structure: matching rules live in
 * {@link MatchingEngine}.
 *
 * <p>Each side is a price ladder: an array of pre-allocated {@link PriceLevel}s indexed by
 * {@code (price - lowPrice) / tickSize}, so finding the level for a price is O(1). A {@link BitSet}
 * per side marks non-empty levels. When the best level empties, the next best is found with
 * {@code nextSetBit}/{@code previousSetBit}, which scan 64 levels per machine word. The best index on
 * each side is cached, so reading the top of book is O(1).
 *
 * <p>This only works because prices are bounded by the static price band. See ADR-0004.
 */
public final class OrderBook {

    private static final int NONE = -1;

    private final SymbolConfig config;
    private final PriceLevel[] bids;
    private final PriceLevel[] asks;
    private final BitSet bidLevels;
    private final BitSet askLevels;
    private int bestBid = NONE;
    private int bestAsk = NONE;
    private int restingOrders;

    /** Per-symbol market data sequence, shared by trades, level updates and top of book. */
    long symbolSeq;

    // Last published L1, used to emit top-of-book only when it changes.
    long publishedBidPrice;
    long publishedBidQuantity;
    long publishedAskPrice;
    long publishedAskQuantity;

    OrderBook(SymbolConfig config) {
        this.config = config;
        int levels = config.levelCount();
        this.bids = new PriceLevel[levels];
        this.asks = new PriceLevel[levels];
        for (int i = 0; i < levels; i++) {
            long price = config.lowPrice() + i * config.tickSize();
            bids[i] = new PriceLevel(price, i);
            asks[i] = new PriceLevel(price, i);
        }
        this.bidLevels = new BitSet(levels);
        this.askLevels = new BitSet(levels);
    }

    public SymbolConfig config() {
        return config;
    }

    /** Ladder index for a price that is already validated to be on the tick grid and inside the band. */
    int indexOf(long price) {
        return (int) ((price - config.lowPrice()) / config.tickSize());
    }

    /** Appends a validated order at the back of its price level's queue. */
    void add(Order order) {
        int index = indexOf(order.price);
        if (order.side == Side.BUY) {
            bids[index].append(order);
            bidLevels.set(index);
            if (index > bestBid) {
                bestBid = index;
            }
        } else {
            asks[index].append(order);
            askLevels.set(index);
            if (bestAsk == NONE || index < bestAsk) {
                bestAsk = index;
            }
        }
        restingOrders++;
    }

    /** Unlinks a resting order, updating the best price if its level empties. Returns its level. */
    PriceLevel remove(Order order) {
        PriceLevel level = order.level;
        level.remove(order);
        restingOrders--;
        if (level.isEmpty()) {
            int index = level.index;
            if (order.side == Side.BUY) {
                bidLevels.clear(index);
                if (index == bestBid) {
                    bestBid = index == 0 ? NONE : bidLevels.previousSetBit(index - 1);
                }
            } else {
                askLevels.clear(index);
                if (index == bestAsk) {
                    bestAsk = askLevels.nextSetBit(index + 1);
                }
            }
        }
        return level;
    }

    /** Best level on {@code side}, or {@code null} if that side is empty. */
    public PriceLevel bestLevel(Side side) {
        if (side == Side.BUY) {
            return bestBid == NONE ? null : bids[bestBid];
        }
        return bestAsk == NONE ? null : asks[bestAsk];
    }

    /** The next level after {@code level} going away from the top of book, or {@code null}. */
    public PriceLevel nextLevel(Side side, PriceLevel level) {
        if (side == Side.BUY) {
            int index = level.index == 0 ? NONE : bidLevels.previousSetBit(level.index - 1);
            return index == NONE ? null : bids[index];
        }
        int index = askLevels.nextSetBit(level.index + 1);
        return index == NONE ? null : asks[index];
    }

    public long bestBidPrice() {
        return bestBid == NONE ? 0 : bids[bestBid].price;
    }

    public long bestBidQuantity() {
        return bestBid == NONE ? 0 : bids[bestBid].totalQuantity;
    }

    public long bestAskPrice() {
        return bestAsk == NONE ? 0 : asks[bestAsk].price;
    }

    public long bestAskQuantity() {
        return bestAsk == NONE ? 0 : asks[bestAsk].totalQuantity;
    }

    public int restingOrders() {
        return restingOrders;
    }

    public long symbolSeq() {
        return symbolSeq;
    }

    /** True if the book has both sides and the best bid is at or above the best ask. Never true after a command. */
    public boolean isCrossed() {
        return bestBid != NONE && bestAsk != NONE && bids[bestBid].price >= asks[bestAsk].price;
    }
}
