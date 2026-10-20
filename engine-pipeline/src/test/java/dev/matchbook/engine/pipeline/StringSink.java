package dev.matchbook.engine.pipeline;

import dev.matchbook.engine.core.BookLevelUpdate;
import dev.matchbook.engine.core.EventSink;
import dev.matchbook.engine.core.ExecutionReport;
import dev.matchbook.engine.core.TopOfBook;
import dev.matchbook.engine.core.TradeEvent;

import java.util.ArrayList;
import java.util.List;

/** Reference sink: the engine's own text for each event, to compare with what the pipeline delivers. */
final class StringSink implements EventSink {

    final List<String> events = new ArrayList<>();

    @Override
    public void onExecutionReport(ExecutionReport report) {
        events.add(report.toString());
    }

    @Override
    public void onTrade(TradeEvent trade) {
        events.add(trade.toString());
    }

    @Override
    public void onBookLevel(BookLevelUpdate update) {
        events.add(update.toString());
    }

    @Override
    public void onTopOfBook(TopOfBook top) {
        events.add(top.toString());
    }
}
