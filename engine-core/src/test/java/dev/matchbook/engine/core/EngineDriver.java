package dev.matchbook.engine.core;

import java.util.List;

/** Thin test helper around an engine and a recording sink. Each call returns the events it caused. */
final class EngineDriver {

    static final int SYMBOL = 1;

    /** tick 1, lot 1, quantity 1..1_000_000, band 1..10_000 (prices are plain integers in tests). */
    static SymbolConfig testSymbol(int symbolId) {
        return new SymbolConfig(symbolId, "SYM" + symbolId, 100, 1, 1, 1, 1_000_000, 1, 10_000);
    }

    final RecordingSink sink = new RecordingSink();
    final MatchingEngine engine;
    private final Command command = new Command();
    private long ts;
    private long clOrdIds;

    EngineDriver() {
        this(new EngineConfig(0, 10_000, List.of(testSymbol(SYMBOL))));
    }

    EngineDriver(EngineConfig config) {
        this.engine = new MatchingEngine(config, sink);
    }

    List<RecordingSink.Event> process(Command c) {
        engine.process(c);
        return sink.drain();
    }

    List<RecordingSink.Event> order(long client, Side side, OrderType type, TimeInForce tif, StpMode stp,
                                    long price, long quantity) {
        return process(command.newOrder(++ts, client, ++clOrdIds, SYMBOL, side, type, tif, stp, price, quantity));
    }

    List<RecordingSink.Event> limit(long client, Side side, long price, long quantity) {
        return order(client, side, OrderType.LIMIT, TimeInForce.GTC, StpMode.CANCEL_RESTING, price, quantity);
    }

    List<RecordingSink.Event> buy(long client, long price, long quantity) {
        return limit(client, Side.BUY, price, quantity);
    }

    List<RecordingSink.Event> sell(long client, long price, long quantity) {
        return limit(client, Side.SELL, price, quantity);
    }

    List<RecordingSink.Event> cancel(long client, long orderId) {
        return process(command.cancel(++ts, client, ++clOrdIds, SYMBOL, orderId));
    }

    List<RecordingSink.Event> replace(long client, long orderId, long price, long quantity) {
        return process(command.replace(++ts, client, ++clOrdIds, SYMBOL, orderId, price, quantity));
    }

    /** The order id assigned by the first report in {@code events} (the NEW for a new order). */
    static long orderId(List<RecordingSink.Event> events) {
        return RecordingSink.execs(events).getFirst().orderId();
    }

    OrderBook book() {
        return engine.orderBook(SYMBOL);
    }
}
