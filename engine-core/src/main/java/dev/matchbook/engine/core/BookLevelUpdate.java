package dev.matchbook.engine.core;

/**
 * L2 depth update: the new aggregate state of one price level. Quantity 0 means the level is gone.
 * Reusable flyweight: copy before returning from the sink.
 */
public final class BookLevelUpdate {

    long seq;
    long symbolSeq;
    long timestamp;
    int symbolId;
    Side side;
    long price;
    long quantity;
    int orderCount;

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

    public Side side() {
        return side;
    }

    public long price() {
        return price;
    }

    public long quantity() {
        return quantity;
    }

    public int orderCount() {
        return orderCount;
    }

    @Override
    public String toString() {
        return "LEVEL seq=" + seq + " symSeq=" + symbolSeq + " sym=" + symbolId + " " + side
                + " px=" + price + " qty=" + quantity + " orders=" + orderCount + " ts=" + timestamp;
    }
}
