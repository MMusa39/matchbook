package dev.matchbook.engine.pipeline;

import dev.matchbook.engine.core.BookLevelUpdate;
import dev.matchbook.engine.core.ExecType;
import dev.matchbook.engine.core.ExecutionReport;
import dev.matchbook.engine.core.Liquidity;
import dev.matchbook.engine.core.OrderType;
import dev.matchbook.engine.core.Reason;
import dev.matchbook.engine.core.Side;
import dev.matchbook.engine.core.TimeInForce;
import dev.matchbook.engine.core.TopOfBook;
import dev.matchbook.engine.core.TradeEvent;

/**
 * One outbound ring-buffer slot: a union of every event the engine emits, plus the context of the
 * command that caused it. Written by the matching thread, read by listeners. Listeners must not keep
 * a reference after {@link OutboundListener#onEvent} returns.
 *
 * <p>Fields that do not apply to the event's {@link #type()} hold stale values from an earlier use of
 * the slot; read only what the type defines.
 */
public final class OutboundEvent {

    public enum Type {
        EXECUTION_REPORT,
        TRADE,
        BOOK_LEVEL,
        TOP_OF_BOOK,
        /** First event of a full book image for one symbol. {@code symbolSeq} is the image's sequence. */
        BOOK_IMAGE_START,
        /** One level of a book image; uses the same fields as {@link #BOOK_LEVEL}. */
        BOOK_IMAGE_LEVEL,
        /** Last event of a book image. */
        BOOK_IMAGE_END
    }

    Type type;

    // Context of the command that produced this event.
    long journalSeq;
    long correlationId;
    long ingressNanos;
    long matchNanos;
    long traceIdHigh;
    long traceIdLow;
    long spanId;
    byte traceFlags;
    /** True on the last event a command produced, so listeners can flush or complete a response. */
    boolean lastOfCommand;

    // Common event fields.
    long seq;
    long symbolSeq;
    long timestamp;
    int symbolId;

    // Execution report.
    ExecType execType;
    Reason reason;
    long orderId;
    long clientId;
    long clOrdId;
    long origClOrdId;
    Side side;
    OrderType orderType;
    TimeInForce timeInForce;
    long price;
    long quantity;
    long lastPrice;
    long lastQuantity;
    long leavesQuantity;
    long cumQuantity;
    long tradeId;
    Liquidity liquidity;

    // Trade.
    Side aggressorSide;
    long buyOrderId;
    long sellOrderId;
    long buyClientId;
    long sellClientId;

    // Book level / top of book.
    int orderCount;
    long bidPrice;
    long bidQuantity;
    long askPrice;
    long askQuantity;

    void setContext(InboundSlot slot, long matchNanos) {
        this.journalSeq = slot.journalSeq;
        this.correlationId = slot.correlationId;
        this.ingressNanos = slot.ingressNanos;
        this.matchNanos = matchNanos;
        this.traceIdHigh = slot.traceIdHigh;
        this.traceIdLow = slot.traceIdLow;
        this.spanId = slot.spanId;
        this.traceFlags = slot.traceFlags;
        this.lastOfCommand = false;
    }

    void copy(ExecutionReport r) {
        type = Type.EXECUTION_REPORT;
        seq = r.seq();
        symbolSeq = 0;
        timestamp = r.timestamp();
        symbolId = r.symbolId();
        execType = r.execType();
        reason = r.reason();
        orderId = r.orderId();
        clientId = r.clientId();
        clOrdId = r.clOrdId();
        origClOrdId = r.origClOrdId();
        side = r.side();
        orderType = r.orderType();
        timeInForce = r.timeInForce();
        price = r.price();
        quantity = r.orderQuantity();
        lastPrice = r.lastPrice();
        lastQuantity = r.lastQuantity();
        leavesQuantity = r.leavesQuantity();
        cumQuantity = r.cumQuantity();
        tradeId = r.tradeId();
        liquidity = r.liquidity();
    }

    void copy(TradeEvent t) {
        type = Type.TRADE;
        seq = t.seq();
        symbolSeq = t.symbolSeq();
        timestamp = t.timestamp();
        symbolId = t.symbolId();
        tradeId = t.tradeId();
        price = t.price();
        quantity = t.quantity();
        aggressorSide = t.aggressorSide();
        buyOrderId = t.buyOrderId();
        sellOrderId = t.sellOrderId();
        buyClientId = t.buyClientId();
        sellClientId = t.sellClientId();
    }

    void copy(BookLevelUpdate u) {
        type = Type.BOOK_LEVEL;
        seq = u.seq();
        symbolSeq = u.symbolSeq();
        timestamp = u.timestamp();
        symbolId = u.symbolId();
        side = u.side();
        price = u.price();
        quantity = u.quantity();
        orderCount = u.orderCount();
    }

    void copy(TopOfBook t) {
        type = Type.TOP_OF_BOOK;
        seq = t.seq();
        symbolSeq = t.symbolSeq();
        timestamp = t.timestamp();
        symbolId = t.symbolId();
        bidPrice = t.bidPrice();
        bidQuantity = t.bidQuantity();
        askPrice = t.askPrice();
        askQuantity = t.askQuantity();
    }

    public Type type() {
        return type;
    }

    public long journalSeq() {
        return journalSeq;
    }

    public long correlationId() {
        return correlationId;
    }

    public long ingressNanos() {
        return ingressNanos;
    }

    public long matchNanos() {
        return matchNanos;
    }

    public long traceIdHigh() {
        return traceIdHigh;
    }

    public long traceIdLow() {
        return traceIdLow;
    }

    public long spanId() {
        return spanId;
    }

    public byte traceFlags() {
        return traceFlags;
    }

    public boolean lastOfCommand() {
        return lastOfCommand;
    }

    public long seq() {
        return seq;
    }

    public long symbolSeq() {
        return symbolSeq;
    }

    public long timestamp() {
        return timestamp;
    }

    public int symbolId() {
        return symbolId;
    }

    public ExecType execType() {
        return execType;
    }

    public Reason reason() {
        return reason;
    }

    public long orderId() {
        return orderId;
    }

    public long clientId() {
        return clientId;
    }

    public long clOrdId() {
        return clOrdId;
    }

    public long origClOrdId() {
        return origClOrdId;
    }

    public Side side() {
        return side;
    }

    public OrderType orderType() {
        return orderType;
    }

    public TimeInForce timeInForce() {
        return timeInForce;
    }

    public long price() {
        return price;
    }

    public long quantity() {
        return quantity;
    }

    public long lastPrice() {
        return lastPrice;
    }

    public long lastQuantity() {
        return lastQuantity;
    }

    public long leavesQuantity() {
        return leavesQuantity;
    }

    public long cumQuantity() {
        return cumQuantity;
    }

    public long tradeId() {
        return tradeId;
    }

    public Liquidity liquidity() {
        return liquidity;
    }

    public Side aggressorSide() {
        return aggressorSide;
    }

    public long buyOrderId() {
        return buyOrderId;
    }

    public long sellOrderId() {
        return sellOrderId;
    }

    public long buyClientId() {
        return buyClientId;
    }

    public long sellClientId() {
        return sellClientId;
    }

    public int orderCount() {
        return orderCount;
    }

    public long bidPrice() {
        return bidPrice;
    }

    public long bidQuantity() {
        return bidQuantity;
    }

    public long askPrice() {
        return askPrice;
    }

    public long askQuantity() {
        return askQuantity;
    }

    /** Readable form, identical to the engine-core event's {@code toString} for engine events. */
    public String describe() {
        return switch (type) {
            case EXECUTION_REPORT -> "EXEC seq=" + seq + " " + execType + " reason=" + reason + " oid=" + orderId
                    + " client=" + clientId + " cl=" + clOrdId + " orig=" + origClOrdId + " sym=" + symbolId
                    + " " + side + " " + orderType + " " + timeInForce + " px=" + price + " qty=" + quantity
                    + " last=" + lastQuantity + "@" + lastPrice + " leaves=" + leavesQuantity + " cum=" + cumQuantity
                    + " trade=" + tradeId + " liq=" + liquidity + " ts=" + timestamp;
            case TRADE -> "TRADE seq=" + seq + " symSeq=" + symbolSeq + " trade=" + tradeId + " sym=" + symbolId
                    + " " + quantity + "@" + price + " aggressor=" + aggressorSide
                    + " buy=" + buyOrderId + "/" + buyClientId + " sell=" + sellOrderId + "/" + sellClientId
                    + " ts=" + timestamp;
            case BOOK_LEVEL -> "LEVEL seq=" + seq + " symSeq=" + symbolSeq + " sym=" + symbolId + " " + side
                    + " px=" + price + " qty=" + quantity + " orders=" + orderCount + " ts=" + timestamp;
            case TOP_OF_BOOK -> "TOP seq=" + seq + " symSeq=" + symbolSeq + " sym=" + symbolId
                    + " bid=" + bidQuantity + "@" + bidPrice + " ask=" + askQuantity + "@" + askPrice
                    + " ts=" + timestamp;
            case BOOK_IMAGE_START -> "IMAGE_START sym=" + symbolId + " symSeq=" + symbolSeq;
            case BOOK_IMAGE_LEVEL -> "IMAGE_LEVEL sym=" + symbolId + " " + side + " px=" + price + " qty=" + quantity
                    + " orders=" + orderCount;
            case BOOK_IMAGE_END -> "IMAGE_END sym=" + symbolId + " symSeq=" + symbolSeq;
        };
    }
}
