package dev.matchbook.engine.core;

import org.agrona.collections.Long2ObjectHashMap;

/**
 * Price-time-priority (FIFO) matching engine for a set of symbols.
 *
 * <p>Not thread-safe by design: exactly one thread calls {@link #process(Command)}. Given the same
 * configuration and the same sequence of commands it produces the same sequence of events, with the
 * same sequence numbers and ids, because it reads no clocks, uses no randomness and never iterates a
 * hash map. In steady state it allocates nothing: orders come from a pre-allocated pool, price levels
 * are pre-allocated, the order-id index is a primitive-keyed open-addressing map sized up front, and
 * events are emitted through reusable flyweights.
 *
 * <p>Event order for one command is fixed:
 * <ol>
 *     <li>the request's first response: {@code NEW}, {@code REJECTED}, {@code CANCELED},
 *     {@code CANCEL_REJECTED}, {@code REPLACED} or {@code REPLACE_REJECTED};</li>
 *     <li>for each fill: a {@link TradeEvent}, then the resting order's report, then the incoming
 *     order's report; self-trade cancels appear in queue order among them;</li>
 *     <li>one {@link BookLevelUpdate} per price level whose aggregate changed, as each level is finished;</li>
 *     <li>a {@code CANCELED} report for any unfilled remainder that does not rest (IOC, FOK, market, STP);</li>
 *     <li>a {@link TopOfBook} if the best bid or offer changed.</li>
 * </ol>
 */
public final class MatchingEngine {

    private final EngineConfig config;
    private final OrderBook[] books;
    private final OrderPool pool;
    private final Long2ObjectHashMap<Order> ordersById;
    private final EventSink sink;
    private final long idBase;

    private final ExecutionReport report = new ExecutionReport();
    private final TradeEvent trade = new TradeEvent();
    private final BookLevelUpdate levelUpdate = new BookLevelUpdate();
    private final TopOfBook top = new TopOfBook();

    private long seq;
    private long orderCounter;
    private long tradeCounter;

    /** Set by {@link #match} when self-trade prevention cancels the remainder of the incoming order. */
    private boolean incomingStpCancelled;

    public MatchingEngine(EngineConfig config, EventSink sink) {
        this.config = config;
        this.sink = sink;
        this.idBase = (long) config.shardId() << EngineConfig.SHARD_SHIFT;
        int maxSymbolId = config.symbols().stream().mapToInt(SymbolConfig::symbolId).max().orElseThrow();
        this.books = new OrderBook[maxSymbolId + 1];
        for (SymbolConfig symbol : config.symbols()) {
            books[symbol.symbolId()] = new OrderBook(symbol);
        }
        this.pool = new OrderPool(config.maxOrders(), config.pooling());
        // Sized so that holding every pooled order never triggers a resize (which would allocate).
        this.ordersById = new Long2ObjectHashMap<>(config.maxOrders() * 2, 0.6f);
    }

    public EngineConfig config() {
        return config;
    }

    /** Processes one command, emitting all of its events to the sink before returning. */
    public void process(Command command) {
        switch (command.type()) {
            case NEW_ORDER -> newOrder(command);
            case CANCEL -> cancel(command);
            case REPLACE -> replace(command);
        }
    }

    // ------------------------------------------------------------------ new order

    private void newOrder(Command c) {
        OrderBook book = book(c.symbolId());
        Reason reject = validateNewOrder(book, c);
        if (reject != Reason.NONE) {
            emitReject(ExecType.REJECTED, reject, c);
            return;
        }
        Order order = pool.acquire();
        if (order == null) {
            emitReject(ExecType.REJECTED, Reason.ORDER_CAPACITY_EXCEEDED, c);
            return;
        }
        order.orderId = idBase + (++orderCounter);
        order.clientId = c.clientId();
        order.clOrdId = c.clOrdId();
        order.symbolId = c.symbolId();
        order.side = c.side();
        order.orderType = c.orderType();
        order.timeInForce = c.timeInForce();
        order.stpMode = c.stpMode();
        order.price = c.orderType() == OrderType.MARKET ? 0 : c.price();
        order.quantity = c.quantity();
        order.leavesQuantity = c.quantity();
        order.cumQuantity = 0;
        order.timestamp = c.timestamp();

        long ts = c.timestamp();
        emitReport(ExecType.NEW, Reason.NONE, order, order.clOrdId, 0, 0, 0, 0, Liquidity.NONE, ts);

        if (order.timeInForce == TimeInForce.FOK && !canFillCompletely(book, order)) {
            cancelIncoming(order, Reason.FOK_NOT_FILLED, ts);
            return;
        }
        match(book, order, ts);
        restOrCancelRemainder(book, order, ts);
        emitTopOfBookIfChanged(book, ts);
    }

    private static Reason validateNewOrder(OrderBook book, Command c) {
        if (book == null) {
            return Reason.UNKNOWN_SYMBOL;
        }
        if (c.side() == null || c.orderType() == null || c.timeInForce() == null || c.stpMode() == null) {
            return Reason.INVALID_FIELD;
        }
        Reason quantity = validateQuantity(book.config(), c.quantity());
        if (quantity != Reason.NONE) {
            return quantity;
        }
        if (c.orderType() == OrderType.MARKET) {
            // A market order has no price to rest at, so it can only be IOC or FOK.
            return c.timeInForce() == TimeInForce.GTC ? Reason.INVALID_TIME_IN_FORCE : Reason.NONE;
        }
        return validatePrice(book.config(), c.price());
    }

    private static Reason validateQuantity(SymbolConfig s, long quantity) {
        if (quantity < s.minQuantity() || quantity > s.maxQuantity()) {
            return Reason.INVALID_QUANTITY;
        }
        if (quantity % s.lotSize() != 0) {
            return Reason.INVALID_LOT_SIZE;
        }
        return Reason.NONE;
    }

    private static Reason validatePrice(SymbolConfig s, long price) {
        if (price <= 0) {
            return Reason.INVALID_PRICE;
        }
        if (price % s.tickSize() != 0) {
            return Reason.INVALID_TICK_SIZE;
        }
        if (price < s.lowPrice() || price > s.highPrice()) {
            return Reason.PRICE_OUT_OF_BAND;
        }
        return Reason.NONE;
    }

    /**
     * FOK pre-check: walks the opposite side in priority order exactly as {@link #match} would and
     * reports whether the whole quantity would fill. Orders of the same client are skipped under
     * {@code CANCEL_RESTING} (matching would cancel them and carry on) and end the walk under the other
     * modes (matching would stop there). Read-only, so a killed FOK order has no side effects.
     */
    private static boolean canFillCompletely(OrderBook book, Order incoming) {
        Side passiveSide = incoming.side.opposite();
        long needed = incoming.leavesQuantity;
        for (PriceLevel level = book.bestLevel(passiveSide);
             level != null && crosses(incoming, level.price);
             level = book.nextLevel(passiveSide, level)) {
            for (Order resting = level.head; resting != null; resting = resting.next) {
                if (resting.clientId == incoming.clientId) {
                    if (incoming.stpMode == StpMode.CANCEL_RESTING) {
                        continue;
                    }
                    return false;
                }
                needed -= resting.leavesQuantity;
                if (needed <= 0) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean crosses(Order incoming, long restingPrice) {
        if (incoming.orderType == OrderType.MARKET) {
            return true;
        }
        return incoming.side == Side.BUY ? incoming.price >= restingPrice : incoming.price <= restingPrice;
    }

    // ------------------------------------------------------------------ matching

    /**
     * Matches {@code incoming} against the opposite side, best price first and oldest first within a
     * price, until it is filled, it no longer crosses, the book side is empty, or self-trade
     * prevention stops it. Trades happen at the resting order's price.
     */
    private void match(OrderBook book, Order incoming, long ts) {
        incomingStpCancelled = false;
        Side passiveSide = incoming.side.opposite();
        while (incoming.leavesQuantity > 0) {
            PriceLevel level = book.bestLevel(passiveSide);
            if (level == null || !crosses(incoming, level.price)) {
                return;
            }
            boolean levelChanged = false;
            Order resting = level.head;
            while (resting != null && incoming.leavesQuantity > 0) {
                Order next = resting.next;
                if (resting.clientId == incoming.clientId) {
                    StpMode mode = incoming.stpMode;
                    if (mode == StpMode.CANCEL_RESTING || mode == StpMode.CANCEL_BOTH) {
                        cancelResting(book, resting, Reason.SELF_TRADE_PREVENTION, ts);
                        levelChanged = true;
                    }
                    if (mode == StpMode.CANCEL_AGGRESSOR || mode == StpMode.CANCEL_BOTH) {
                        incomingStpCancelled = true;
                        break;
                    }
                } else {
                    fill(book, incoming, resting, level, ts);
                    levelChanged = true;
                }
                resting = next;
            }
            if (levelChanged) {
                emitLevel(book, passiveSide, level, ts);
            }
            if (incomingStpCancelled) {
                return;
            }
        }
    }

    private void fill(OrderBook book, Order incoming, Order resting, PriceLevel level, long ts) {
        long quantity = Math.min(incoming.leavesQuantity, resting.leavesQuantity);
        long price = level.price;
        long tradeId = idBase + (++tradeCounter);

        resting.leavesQuantity -= quantity;
        resting.cumQuantity += quantity;
        level.totalQuantity -= quantity;
        incoming.leavesQuantity -= quantity;
        incoming.cumQuantity += quantity;

        emitTrade(book, incoming, resting, price, quantity, tradeId, ts);
        emitReport(resting.leavesQuantity == 0 ? ExecType.FILL : ExecType.PARTIAL_FILL, Reason.NONE, resting,
                resting.clOrdId, 0, price, quantity, tradeId, Liquidity.MAKER, ts);
        emitReport(incoming.leavesQuantity == 0 ? ExecType.FILL : ExecType.PARTIAL_FILL, Reason.NONE, incoming,
                incoming.clOrdId, 0, price, quantity, tradeId, Liquidity.TAKER, ts);

        if (resting.leavesQuantity == 0) {
            book.remove(resting);
            ordersById.remove(resting.orderId);
            pool.release(resting);
        }
    }

    /** After matching: rest a GTC limit remainder, or cancel whatever is left. */
    private void restOrCancelRemainder(OrderBook book, Order order, long ts) {
        if (order.leavesQuantity == 0) {
            pool.release(order);
        } else if (incomingStpCancelled) {
            cancelIncoming(order, Reason.SELF_TRADE_PREVENTION, ts);
        } else if (order.orderType == OrderType.LIMIT && order.timeInForce == TimeInForce.GTC) {
            book.add(order);
            ordersById.put(order.orderId, order);
            emitLevel(book, order.side, order.level, ts);
        } else {
            cancelIncoming(order, Reason.IOC_REMAINDER, ts);
        }
    }

    /** Cancels an order that is not in the book (the incoming order's remainder) and frees it. */
    private void cancelIncoming(Order order, Reason reason, long ts) {
        order.leavesQuantity = 0;
        emitReport(ExecType.CANCELED, reason, order, order.clOrdId, 0, 0, 0, 0, Liquidity.NONE, ts);
        pool.release(order);
    }

    /**
     * Cancels a resting order. The level update is left to the caller so a sweep emits one update per
     * level. The cancelled quantity is {@code orderQuantity - cumQuantity} on the report.
     */
    private void cancelResting(OrderBook book, Order order, Reason reason, long ts) {
        book.remove(order);
        ordersById.remove(order.orderId);
        order.leavesQuantity = 0;
        emitReport(ExecType.CANCELED, reason, order, order.clOrdId, 0, 0, 0, 0, Liquidity.NONE, ts);
        pool.release(order);
    }

    // ------------------------------------------------------------------ cancel

    private void cancel(Command c) {
        Order order = ownedOrder(c);
        if (order == null) {
            emitReject(ExecType.CANCEL_REJECTED, Reason.UNKNOWN_ORDER, c);
            return;
        }
        OrderBook book = books[order.symbolId];
        long ts = c.timestamp();
        long origClOrdId = order.clOrdId;
        PriceLevel level = book.remove(order);
        ordersById.remove(order.orderId);
        order.leavesQuantity = 0;
        emitReport(ExecType.CANCELED, Reason.CLIENT_CANCEL, order, c.clOrdId(), origClOrdId, 0, 0, 0,
                Liquidity.NONE, ts);
        emitLevel(book, order.side, level, ts);
        pool.release(order);
        emitTopOfBookIfChanged(book, ts);
    }

    /** The resting order named by a cancel/replace, if it exists and belongs to the sender. */
    private Order ownedOrder(Command c) {
        Order order = ordersById.get(c.orderId());
        if (order == null || order.clientId != c.clientId() || order.symbolId != c.symbolId()) {
            // Someone else's order is reported as unknown so its existence is not revealed.
            return null;
        }
        return order;
    }

    // ------------------------------------------------------------------ replace

    /**
     * Cancel/replace. A quantity decrease at the same price amends the order in place and keeps its
     * queue position. Any price change or quantity increase takes the order out of the book and
     * re-enters it as if new (same order id, new timestamp), so it goes to the back of the queue and
     * can trade immediately if the new price crosses.
     */
    private void replace(Command c) {
        Order order = ownedOrder(c);
        if (order == null) {
            emitReject(ExecType.REPLACE_REJECTED, Reason.UNKNOWN_ORDER, c);
            return;
        }
        OrderBook book = books[order.symbolId];
        SymbolConfig symbol = book.config();
        Reason reject = validatePrice(symbol, c.price());
        if (reject == Reason.NONE) {
            reject = validateQuantity(symbol, c.quantity());
        }
        if (reject == Reason.NONE && c.quantity() <= order.cumQuantity) {
            reject = Reason.INVALID_QUANTITY;
        }
        if (reject != Reason.NONE) {
            emitRejectForOrder(ExecType.REPLACE_REJECTED, reject, order, c);
            return;
        }

        long ts = c.timestamp();
        long origClOrdId = order.clOrdId;
        long newLeaves = c.quantity() - order.cumQuantity;
        order.clOrdId = c.clOrdId();

        if (c.price() == order.price && newLeaves <= order.leavesQuantity) {
            long reduction = order.leavesQuantity - newLeaves;
            order.leavesQuantity = newLeaves;
            order.quantity = c.quantity();
            order.level.totalQuantity -= reduction;
            emitReport(ExecType.REPLACED, Reason.NONE, order, order.clOrdId, origClOrdId, 0, 0, 0,
                    Liquidity.NONE, ts);
            if (reduction > 0) {
                emitLevel(book, order.side, order.level, ts);
            }
            emitTopOfBookIfChanged(book, ts);
            return;
        }

        PriceLevel oldLevel = book.remove(order);
        ordersById.remove(order.orderId);
        order.price = c.price();
        order.quantity = c.quantity();
        order.leavesQuantity = newLeaves;
        order.timestamp = ts;
        emitReport(ExecType.REPLACED, Reason.NONE, order, order.clOrdId, origClOrdId, 0, 0, 0,
                Liquidity.NONE, ts);
        emitLevel(book, order.side, oldLevel, ts);
        match(book, order, ts);
        restOrCancelRemainder(book, order, ts);
        emitTopOfBookIfChanged(book, ts);
    }

    // ------------------------------------------------------------------ event emission

    private void emitReport(ExecType type, Reason reason, Order o, long clOrdId, long origClOrdId,
                            long lastPrice, long lastQuantity, long tradeId, Liquidity liquidity, long ts) {
        ExecutionReport r = report;
        r.seq = ++seq;
        r.timestamp = ts;
        r.execType = type;
        r.reason = reason;
        r.orderId = o.orderId;
        r.clientId = o.clientId;
        r.clOrdId = clOrdId;
        r.origClOrdId = origClOrdId;
        r.symbolId = o.symbolId;
        r.side = o.side;
        r.orderType = o.orderType;
        r.timeInForce = o.timeInForce;
        r.price = o.price;
        r.orderQuantity = o.quantity;
        r.lastPrice = lastPrice;
        r.lastQuantity = lastQuantity;
        r.leavesQuantity = o.leavesQuantity;
        r.cumQuantity = o.cumQuantity;
        r.tradeId = tradeId;
        r.liquidity = liquidity;
        sink.onExecutionReport(r);
    }

    /** Reject of a command that has no live order behind it: echo what the request carried. */
    private void emitReject(ExecType type, Reason reason, Command c) {
        ExecutionReport r = report;
        r.seq = ++seq;
        r.timestamp = c.timestamp();
        r.execType = type;
        r.reason = reason;
        r.orderId = c.orderId();
        r.clientId = c.clientId();
        r.clOrdId = c.clOrdId();
        r.origClOrdId = 0;
        r.symbolId = c.symbolId();
        r.side = c.side();
        r.orderType = c.orderType();
        r.timeInForce = c.timeInForce();
        r.price = c.price();
        r.orderQuantity = c.quantity();
        r.lastPrice = 0;
        r.lastQuantity = 0;
        r.leavesQuantity = 0;
        r.cumQuantity = 0;
        r.tradeId = 0;
        r.liquidity = Liquidity.NONE;
        sink.onExecutionReport(r);
    }

    /** Reject of a replace for a live order: report the order's unchanged state. */
    private void emitRejectForOrder(ExecType type, Reason reason, Order o, Command c) {
        emitReport(type, reason, o, c.clOrdId(), o.clOrdId, 0, 0, 0, Liquidity.NONE, c.timestamp());
    }

    private void emitTrade(OrderBook book, Order incoming, Order resting, long price, long quantity,
                           long tradeId, long ts) {
        boolean incomingBuys = incoming.side == Side.BUY;
        TradeEvent t = trade;
        t.seq = ++seq;
        t.symbolSeq = ++book.symbolSeq;
        t.timestamp = ts;
        t.tradeId = tradeId;
        t.symbolId = incoming.symbolId;
        t.price = price;
        t.quantity = quantity;
        t.aggressorSide = incoming.side;
        t.buyOrderId = incomingBuys ? incoming.orderId : resting.orderId;
        t.sellOrderId = incomingBuys ? resting.orderId : incoming.orderId;
        t.buyClientId = incomingBuys ? incoming.clientId : resting.clientId;
        t.sellClientId = incomingBuys ? resting.clientId : incoming.clientId;
        sink.onTrade(t);
    }

    private void emitLevel(OrderBook book, Side side, PriceLevel level, long ts) {
        BookLevelUpdate u = levelUpdate;
        u.seq = ++seq;
        u.symbolSeq = ++book.symbolSeq;
        u.timestamp = ts;
        u.symbolId = book.config().symbolId();
        u.side = side;
        u.price = level.price;
        u.quantity = level.totalQuantity;
        u.orderCount = level.orderCount;
        sink.onBookLevel(u);
    }

    private void emitTopOfBookIfChanged(OrderBook book, long ts) {
        long bidPrice = book.bestBidPrice();
        long bidQuantity = book.bestBidQuantity();
        long askPrice = book.bestAskPrice();
        long askQuantity = book.bestAskQuantity();
        if (bidPrice == book.publishedBidPrice && bidQuantity == book.publishedBidQuantity
                && askPrice == book.publishedAskPrice && askQuantity == book.publishedAskQuantity) {
            return;
        }
        book.publishedBidPrice = bidPrice;
        book.publishedBidQuantity = bidQuantity;
        book.publishedAskPrice = askPrice;
        book.publishedAskQuantity = askQuantity;
        TopOfBook t = top;
        t.seq = ++seq;
        t.symbolSeq = ++book.symbolSeq;
        t.timestamp = ts;
        t.symbolId = book.config().symbolId();
        t.bidPrice = bidPrice;
        t.bidQuantity = bidQuantity;
        t.askPrice = askPrice;
        t.askQuantity = askQuantity;
        sink.onTopOfBook(t);
    }

    // ------------------------------------------------------------------ queries (not for the hot path)

    private OrderBook book(int symbolId) {
        return symbolId >= 0 && symbolId < books.length ? books[symbolId] : null;
    }

    /** The book for a symbol, or {@code null}. Read-only use; the engine thread owns it. */
    public OrderBook orderBook(int symbolId) {
        return book(symbolId);
    }

    /** A resting order by id, or {@code null}. */
    public Order restingOrder(long orderId) {
        return ordersById.get(orderId);
    }

    /** The id given to the most recently accepted order, or 0 if none yet. */
    public long lastOrderId() {
        return orderCounter == 0 ? 0 : idBase + orderCounter;
    }

    /** Sequence number of the last event emitted. */
    public long lastSeq() {
        return seq;
    }

    public int restingOrderCount() {
        return ordersById.size();
    }

    public int freeOrderCapacity() {
        return pool.available();
    }

    // ------------------------------------------------------------------ snapshot support

    OrderBook[] books() {
        return books;
    }

    long orderCounter() {
        return orderCounter;
    }

    long tradeCounter() {
        return tradeCounter;
    }

    void restoreCounters(long seq, long orderCounter, long tradeCounter) {
        this.seq = seq;
        this.orderCounter = orderCounter;
        this.tradeCounter = tradeCounter;
    }

    /** Re-inserts a resting order during snapshot load. Orders must arrive in queue order. */
    void restoreRestingOrder(Order template) {
        Order order = pool.acquire();
        if (order == null) {
            throw new IllegalStateException("snapshot holds more orders than maxOrders");
        }
        order.orderId = template.orderId;
        order.clientId = template.clientId;
        order.clOrdId = template.clOrdId;
        order.symbolId = template.symbolId;
        order.side = template.side;
        order.orderType = template.orderType;
        order.timeInForce = template.timeInForce;
        order.stpMode = template.stpMode;
        order.price = template.price;
        order.quantity = template.quantity;
        order.leavesQuantity = template.leavesQuantity;
        order.cumQuantity = template.cumQuantity;
        order.timestamp = template.timestamp;
        books[order.symbolId].add(order);
        ordersById.put(order.orderId, order);
    }

    boolean isEmpty() {
        return ordersById.isEmpty() && seq == 0;
    }
}
