package dev.matchbook.engine.core;

/**
 * Public trade print. Reusable flyweight: copy before returning from the sink.
 * {@code symbolSeq} is the per-symbol market data sequence number shared with book updates, so a
 * market data consumer can detect gaps per symbol.
 */
public final class TradeEvent {

    long seq;
    long symbolSeq;
    long timestamp;
    long tradeId;
    int symbolId;
    long price;
    long quantity;
    Side aggressorSide;
    long buyOrderId;
    long sellOrderId;
    long buyClientId;
    long sellClientId;

    public long seq() {
        return seq;
    }

    public long symbolSeq() {
        return symbolSeq;
    }

    public long timestamp() {
        return timestamp;
    }

    public long tradeId() {
        return tradeId;
    }

    public int symbolId() {
        return symbolId;
    }

    public long price() {
        return price;
    }

    public long quantity() {
        return quantity;
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

    @Override
    public String toString() {
        return "TRADE seq=" + seq + " symSeq=" + symbolSeq + " trade=" + tradeId + " sym=" + symbolId
                + " " + quantity + "@" + price + " aggressor=" + aggressorSide
                + " buy=" + buyOrderId + "/" + buyClientId + " sell=" + sellOrderId + "/" + sellClientId
                + " ts=" + timestamp;
    }
}
