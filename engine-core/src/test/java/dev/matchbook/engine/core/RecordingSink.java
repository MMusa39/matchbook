package dev.matchbook.engine.core;

import java.util.ArrayList;
import java.util.List;

/** Test sink that copies every flyweight into an immutable record, in emission order. */
final class RecordingSink implements EventSink {

    sealed interface Event permits Exec, Trade, Level, Top {
        long seq();
    }

    record Exec(long seq, ExecType type, Reason reason, long orderId, long clientId, long clOrdId,
                long origClOrdId, int symbolId, Side side, OrderType orderType, TimeInForce tif, long price,
                long quantity, long lastPrice, long lastQuantity, long leaves, long cum, long tradeId,
                Liquidity liquidity, long timestamp, String text) implements Event {
        @Override
        public String toString() {
            return text;
        }
    }

    record Trade(long seq, long symbolSeq, long tradeId, int symbolId, long price, long quantity, Side aggressor,
                 long buyOrderId, long sellOrderId, long buyClientId, long sellClientId, String text)
            implements Event {
        @Override
        public String toString() {
            return text;
        }
    }

    record Level(long seq, long symbolSeq, int symbolId, Side side, long price, long quantity, int orders,
                 String text) implements Event {
        @Override
        public String toString() {
            return text;
        }
    }

    record Top(long seq, long symbolSeq, int symbolId, long bidPrice, long bidQuantity, long askPrice,
               long askQuantity, String text) implements Event {
        @Override
        public String toString() {
            return text;
        }
    }

    final List<Event> events = new ArrayList<>();

    @Override
    public void onExecutionReport(ExecutionReport r) {
        events.add(new Exec(r.seq(), r.execType(), r.reason(), r.orderId(), r.clientId(), r.clOrdId(),
                r.origClOrdId(), r.symbolId(), r.side(), r.orderType(), r.timeInForce(), r.price(),
                r.orderQuantity(), r.lastPrice(), r.lastQuantity(), r.leavesQuantity(), r.cumQuantity(),
                r.tradeId(), r.liquidity(), r.timestamp(), r.toString()));
    }

    @Override
    public void onTrade(TradeEvent t) {
        events.add(new Trade(t.seq(), t.symbolSeq(), t.tradeId(), t.symbolId(), t.price(), t.quantity(),
                t.aggressorSide(), t.buyOrderId(), t.sellOrderId(), t.buyClientId(), t.sellClientId(),
                t.toString()));
    }

    @Override
    public void onBookLevel(BookLevelUpdate u) {
        events.add(new Level(u.seq(), u.symbolSeq(), u.symbolId(), u.side(), u.price(), u.quantity(),
                u.orderCount(), u.toString()));
    }

    @Override
    public void onTopOfBook(TopOfBook t) {
        events.add(new Top(t.seq(), t.symbolSeq(), t.symbolId(), t.bidPrice(), t.bidQuantity(), t.askPrice(),
                t.askQuantity(), t.toString()));
    }

    /** Removes and returns everything recorded so far. */
    List<Event> drain() {
        List<Event> copy = List.copyOf(events);
        events.clear();
        return copy;
    }

    static List<Exec> execs(List<Event> events) {
        return events.stream().filter(Exec.class::isInstance).map(Exec.class::cast).toList();
    }

    static List<Trade> trades(List<Event> events) {
        return events.stream().filter(Trade.class::isInstance).map(Trade.class::cast).toList();
    }

    static List<Level> levels(List<Event> events) {
        return events.stream().filter(Level.class::isInstance).map(Level.class::cast).toList();
    }

    static List<Top> tops(List<Event> events) {
        return events.stream().filter(Top.class::isInstance).map(Top.class::cast).toList();
    }
}
