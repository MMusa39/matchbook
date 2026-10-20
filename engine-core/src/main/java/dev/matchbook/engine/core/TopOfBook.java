package dev.matchbook.engine.core;

/**
 * L1 update, emitted at the end of a command when the best bid or best offer (price or size) changed.
 * A price and quantity of 0 mean that side of the book is empty. Reusable flyweight: copy before
 * returning from the sink.
 */
public final class TopOfBook {

    long seq;
    long symbolSeq;
    long timestamp;
    int symbolId;
    long bidPrice;
    long bidQuantity;
    long askPrice;
    long askQuantity;

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

    @Override
    public String toString() {
        return "TOP seq=" + seq + " symSeq=" + symbolSeq + " sym=" + symbolId
                + " bid=" + bidQuantity + "@" + bidPrice + " ask=" + askQuantity + "@" + askPrice
                + " ts=" + timestamp;
    }
}
