package dev.matchbook.engine.pipeline;

import dev.matchbook.engine.core.BookLevelUpdate;
import dev.matchbook.engine.core.Command;
import dev.matchbook.engine.core.EventSink;
import dev.matchbook.engine.core.ExecType;
import dev.matchbook.engine.core.ExecutionReport;
import dev.matchbook.engine.core.MatchingEngine;
import dev.matchbook.engine.core.TopOfBook;
import dev.matchbook.engine.core.TradeEvent;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SyntheticOrderFlowTest {

    @Test
    void producesSteadyStateFlowWithPredictableIds() {
        Map<ExecType, Long> counts = new EnumMap<>(ExecType.class);
        long[] trades = new long[1];
        EventSink sink = new EventSink() {
            @Override
            public void onExecutionReport(ExecutionReport r) {
                counts.merge(r.execType(), 1L, Long::sum);
            }

            @Override
            public void onTrade(TradeEvent t) {
                trades[0]++;
            }

            @Override
            public void onBookLevel(BookLevelUpdate u) {
            }

            @Override
            public void onTopOfBook(TopOfBook t) {
            }
        };
        MatchingEngine engine = new MatchingEngine(SyntheticOrderFlow.engineConfig(1 << 16, true), sink);
        SyntheticOrderFlow workload = new SyntheticOrderFlow(42, 4096);
        Command command = new Command();
        int maxResting = 0;
        for (int i = 1; i <= 2_000_000; i++) {
            engine.process(workload.next(command, i));
            maxResting = Math.max(maxResting, engine.restingOrderCount());
        }

        assertThat(counts.getOrDefault(ExecType.REJECTED, 0L)).as("every new order is valid").isZero();
        assertThat(maxResting).isLessThanOrEqualTo(workload.ringCapacity());
        assertThat(engine.restingOrderCount()).isGreaterThan(1000);
        long cancels = counts.getOrDefault(ExecType.CANCELED, 0L);
        long cancelRejects = counts.getOrDefault(ExecType.CANCEL_REJECTED, 0L);
        assertThat(cancels).as("most cancels hit live orders").isGreaterThan(cancelRejects);
        assertThat(counts.getOrDefault(ExecType.REPLACED, 0L)).isPositive();
        assertThat(trades[0]).isGreaterThan(100_000);
        assertThat(engine.orderBook(SyntheticOrderFlow.SYMBOL).isCrossed()).isFalse();
        System.out.printf("counts=%s trades=%d maxResting=%d finalResting=%d%n",
                counts, trades[0], maxResting, engine.restingOrderCount());
    }
}
