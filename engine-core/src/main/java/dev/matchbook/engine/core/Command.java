package dev.matchbook.engine.core;

/**
 * A mutable, reusable engine input. The pipeline keeps one instance per ring-buffer slot and
 * overwrites it, so nothing is allocated per command.
 *
 * <p>Field meaning by type:
 * <ul>
 *     <li>{@link CommandType#NEW_ORDER}: side, orderType, timeInForce, stpMode, price (ignored for
 *     market orders), quantity. {@code clOrdId} identifies the new order.</li>
 *     <li>{@link CommandType#CANCEL}: orderId of the resting order. {@code clOrdId} identifies the
 *     cancel request.</li>
 *     <li>{@link CommandType#REPLACE}: orderId, new price, new total quantity (FIX OrderQty semantics:
 *     includes what has already been filled). {@code clOrdId} becomes the order's new client id.</li>
 * </ul>
 * {@code timestamp} is supplied by the caller; the engine never reads a clock, which keeps it
 * deterministic.
 */
public final class Command {

    private CommandType type;
    private long timestamp;
    private long clientId;
    private long clOrdId;
    private int symbolId;
    private Side side;
    private OrderType orderType;
    private TimeInForce timeInForce;
    private StpMode stpMode;
    private long price;
    private long quantity;
    private long orderId;

    public Command newOrder(long timestamp, long clientId, long clOrdId, int symbolId, Side side,
                            OrderType orderType, TimeInForce timeInForce, StpMode stpMode,
                            long price, long quantity) {
        this.type = CommandType.NEW_ORDER;
        this.timestamp = timestamp;
        this.clientId = clientId;
        this.clOrdId = clOrdId;
        this.symbolId = symbolId;
        this.side = side;
        this.orderType = orderType;
        this.timeInForce = timeInForce;
        this.stpMode = stpMode;
        this.price = price;
        this.quantity = quantity;
        this.orderId = 0;
        return this;
    }

    public Command cancel(long timestamp, long clientId, long clOrdId, int symbolId, long orderId) {
        this.type = CommandType.CANCEL;
        this.timestamp = timestamp;
        this.clientId = clientId;
        this.clOrdId = clOrdId;
        this.symbolId = symbolId;
        this.side = null;
        this.orderType = null;
        this.timeInForce = null;
        this.stpMode = null;
        this.price = 0;
        this.quantity = 0;
        this.orderId = orderId;
        return this;
    }

    public Command replace(long timestamp, long clientId, long clOrdId, int symbolId, long orderId,
                           long price, long quantity) {
        this.type = CommandType.REPLACE;
        this.timestamp = timestamp;
        this.clientId = clientId;
        this.clOrdId = clOrdId;
        this.symbolId = symbolId;
        this.side = null;
        this.orderType = null;
        this.timeInForce = null;
        this.stpMode = null;
        this.price = price;
        this.quantity = quantity;
        this.orderId = orderId;
        return this;
    }

    /** Copies every field from {@code other}. */
    public Command copyFrom(Command other) {
        this.type = other.type;
        this.timestamp = other.timestamp;
        this.clientId = other.clientId;
        this.clOrdId = other.clOrdId;
        this.symbolId = other.symbolId;
        this.side = other.side;
        this.orderType = other.orderType;
        this.timeInForce = other.timeInForce;
        this.stpMode = other.stpMode;
        this.price = other.price;
        this.quantity = other.quantity;
        this.orderId = other.orderId;
        return this;
    }

    public CommandType type() {
        return type;
    }

    public long timestamp() {
        return timestamp;
    }

    public long clientId() {
        return clientId;
    }

    public long clOrdId() {
        return clOrdId;
    }

    public int symbolId() {
        return symbolId;
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

    public StpMode stpMode() {
        return stpMode;
    }

    public long price() {
        return price;
    }

    public long quantity() {
        return quantity;
    }

    public long orderId() {
        return orderId;
    }

    @Override
    public String toString() {
        return switch (type) {
            case NEW_ORDER -> "NEW ts=" + timestamp + " client=" + clientId + " cl=" + clOrdId
                    + " sym=" + symbolId + " side=" + side + " type=" + orderType + " tif=" + timeInForce
                    + " stp=" + stpMode + " px=" + price + " qty=" + quantity;
            case CANCEL -> "CANCEL ts=" + timestamp + " client=" + clientId + " cl=" + clOrdId
                    + " sym=" + symbolId + " oid=" + orderId;
            case REPLACE -> "REPLACE ts=" + timestamp + " client=" + clientId + " cl=" + clOrdId
                    + " sym=" + symbolId + " oid=" + orderId + " px=" + price + " qty=" + quantity;
            case null -> "EMPTY";
        };
    }
}
