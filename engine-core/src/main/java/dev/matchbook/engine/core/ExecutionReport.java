package dev.matchbook.engine.core;

/**
 * Private execution report sent to the owner of an order. This is a reusable flyweight: the engine
 * overwrites the same instance for every report, so a sink must copy what it needs before returning.
 *
 * <p>{@code clOrdId} is the client id of the request that caused the report (the cancel request id for
 * a cancel, the new id for a replace). {@code origClOrdId} is the order's previous client id on
 * cancel and replace reports, otherwise 0.
 */
public final class ExecutionReport {

    long seq;
    long timestamp;
    ExecType execType;
    Reason reason;
    long orderId;
    long clientId;
    long clOrdId;
    long origClOrdId;
    int symbolId;
    Side side;
    OrderType orderType;
    TimeInForce timeInForce;
    long price;
    long orderQuantity;
    long lastPrice;
    long lastQuantity;
    long leavesQuantity;
    long cumQuantity;
    long tradeId;
    Liquidity liquidity;

    public long seq() {
        return seq;
    }

    public long timestamp() {
        return timestamp;
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

    public long price() {
        return price;
    }

    public long orderQuantity() {
        return orderQuantity;
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

    @Override
    public String toString() {
        return "EXEC seq=" + seq + " " + execType + " reason=" + reason + " oid=" + orderId
                + " client=" + clientId + " cl=" + clOrdId + " orig=" + origClOrdId + " sym=" + symbolId
                + " " + side + " " + orderType + " " + timeInForce + " px=" + price + " qty=" + orderQuantity
                + " last=" + lastQuantity + "@" + lastPrice + " leaves=" + leavesQuantity + " cum=" + cumQuantity
                + " trade=" + tradeId + " liq=" + liquidity + " ts=" + timestamp;
    }
}
