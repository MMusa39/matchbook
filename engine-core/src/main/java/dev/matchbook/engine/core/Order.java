package dev.matchbook.engine.core;

/**
 * A live order. Instances are pooled and reused, and are also the nodes of their price level's
 * intrusive doubly linked FIFO ({@link #prev}/{@link #next}), so resting an order or cancelling it
 * allocates nothing and needs no separate list node.
 */
public final class Order {

    long orderId;
    long clientId;
    long clOrdId;
    int symbolId;
    Side side;
    OrderType orderType;
    TimeInForce timeInForce;
    StpMode stpMode;
    long price;
    /** Total order quantity, FIX OrderQty. Always {@code leavesQuantity + cumQuantity} while live. */
    long quantity;
    long leavesQuantity;
    long cumQuantity;
    /** Timestamp at which the order got its current queue priority. */
    long timestamp;

    // Intrusive queue links. Also reused by OrderPool as the free-list link.
    Order prev;
    Order next;
    PriceLevel level;

    void reset() {
        orderId = 0;
        clientId = 0;
        clOrdId = 0;
        symbolId = 0;
        side = null;
        orderType = null;
        timeInForce = null;
        stpMode = null;
        price = 0;
        quantity = 0;
        leavesQuantity = 0;
        cumQuantity = 0;
        timestamp = 0;
        prev = null;
        next = null;
        level = null;
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

    public long leavesQuantity() {
        return leavesQuantity;
    }

    public long cumQuantity() {
        return cumQuantity;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Order{id=" + orderId + ", client=" + clientId + ", cl=" + clOrdId + ", " + side
                + ", px=" + price + ", qty=" + quantity + ", leaves=" + leavesQuantity
                + ", cum=" + cumQuantity + '}';
    }
}
