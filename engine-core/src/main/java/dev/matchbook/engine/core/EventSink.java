package dev.matchbook.engine.core;

/**
 * Receives engine output synchronously on the matching thread, in sequence order.
 *
 * <p>Each argument is a flyweight that the engine reuses for the next event, so an implementation
 * must copy what it needs (for example into a ring-buffer slot) before returning, and must not keep
 * a reference. Implementations on the hot path must not allocate or block.
 */
public interface EventSink {

    void onExecutionReport(ExecutionReport report);

    void onTrade(TradeEvent trade);

    void onBookLevel(BookLevelUpdate update);

    void onTopOfBook(TopOfBook top);

    /** A sink that drops everything. Useful for benchmarks that only measure matching. */
    EventSink NO_OP = new EventSink() {
        @Override
        public void onExecutionReport(ExecutionReport report) {
        }

        @Override
        public void onTrade(TradeEvent trade) {
        }

        @Override
        public void onBookLevel(BookLevelUpdate update) {
        }

        @Override
        public void onTopOfBook(TopOfBook top) {
        }
    };
}
