package dev.matchbook.bench;

import dev.matchbook.engine.core.BookLevelUpdate;
import dev.matchbook.engine.core.EventSink;
import dev.matchbook.engine.core.ExecutionReport;
import dev.matchbook.engine.core.TopOfBook;
import dev.matchbook.engine.core.TradeEvent;
import org.openjdk.jmh.infra.Blackhole;

/** Sink that feeds a field of every event to a JMH {@link Blackhole}, so emission cannot be optimised away. */
final class BlackholeSink implements EventSink {

    private final Blackhole bh;
    long events;

    BlackholeSink(Blackhole bh) {
        this.bh = bh;
    }

    @Override
    public void onExecutionReport(ExecutionReport report) {
        events++;
        bh.consume(report.leavesQuantity());
    }

    @Override
    public void onTrade(TradeEvent trade) {
        events++;
        bh.consume(trade.quantity());
    }

    @Override
    public void onBookLevel(BookLevelUpdate update) {
        events++;
        bh.consume(update.quantity());
    }

    @Override
    public void onTopOfBook(TopOfBook top) {
        events++;
        bh.consume(top.bidPrice());
    }
}
