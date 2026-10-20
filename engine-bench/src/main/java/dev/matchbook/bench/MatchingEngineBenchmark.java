package dev.matchbook.bench;

import dev.matchbook.engine.pipeline.SyntheticOrderFlow;

import dev.matchbook.engine.core.Command;
import dev.matchbook.engine.core.MatchingEngine;
import dev.matchbook.engine.core.OrderType;
import dev.matchbook.engine.core.Side;
import dev.matchbook.engine.core.StpMode;
import dev.matchbook.engine.core.TimeInForce;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * Single-threaded matching cost, with no pipeline, journal or I/O. Each benchmark method is one
 * engine command unless marked with {@link OperationsPerInvocation}, so scores are per command.
 *
 * <p>Run with {@code -prof gc} to see allocation per command ({@code gc.alloc.rate.norm}).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 3, jvmArgsAppend = {"-Xms2g", "-Xmx2g", "-XX:+AlwaysPreTouch"})
@State(Scope.Thread)
public class MatchingEngineBenchmark {

    @Param({"true", "false"})
    public boolean pooling;

    private MatchingEngine engine;
    private SyntheticOrderFlow workload;
    private final Command command = new Command();
    private long ts;

    @Setup(Level.Trial)
    public void setUp(Blackhole bh) {
        engine = new MatchingEngine(SyntheticOrderFlow.engineConfig(1 << 16, pooling), new BlackholeSink(bh));
        workload = new SyntheticOrderFlow(42, 4096);
        // Fill the book to its steady-state depth before measuring.
        for (int i = 0; i < 200_000; i++) {
            engine.process(workload.next(command, ++ts));
        }
    }

    /** Realistic mix: passive adds, cancels, replaces and aggressive IOC orders against a ~4k-order book. */
    @Benchmark
    public void mixedWorkload() {
        engine.process(workload.next(command, ++ts));
    }

    /** A passive order that rests behind the steady-state bids, then its cancel. */
    @Benchmark
    @OperationsPerInvocation(2)
    public void addThenCancel() {
        long t = ++ts;
        engine.process(command.newOrder(t, 99, t, SyntheticOrderFlow.SYMBOL, Side.BUY, OrderType.LIMIT, TimeInForce.GTC,
                StpMode.CANCEL_RESTING, SyntheticOrderFlow.MID - 60, 10));
        engine.process(command.cancel(t, 99, t, SyntheticOrderFlow.SYMBOL, engine.lastOrderId()));
    }

    /** A resting order fully filled by the next order, on an otherwise empty book: one trade per pair. */
    @Benchmark
    @OperationsPerInvocation(2)
    public void restThenFullFill() {
        long t = ++ts;
        engine.process(command.newOrder(t, 98, t, SyntheticOrderFlow.QUIET_SYMBOL, Side.SELL, OrderType.LIMIT,
                TimeInForce.GTC, StpMode.CANCEL_RESTING, SyntheticOrderFlow.MID, 10));
        engine.process(command.newOrder(t, 97, t, SyntheticOrderFlow.QUIET_SYMBOL, Side.BUY, OrderType.LIMIT,
                TimeInForce.IOC, StpMode.CANCEL_RESTING, SyntheticOrderFlow.MID, 10));
    }

    /** Ten resting levels swept by one aggressive order: ten trades and ten level updates per sweep. */
    @Benchmark
    @OperationsPerInvocation(11)
    public void sweepTenLevels() {
        long t = ++ts;
        for (int i = 0; i < 10; i++) {
            engine.process(command.newOrder(t, 96, t, SyntheticOrderFlow.QUIET_SYMBOL, Side.SELL, OrderType.LIMIT,
                    TimeInForce.GTC, StpMode.CANCEL_RESTING, SyntheticOrderFlow.MID + i, 10));
        }
        engine.process(command.newOrder(t, 95, t, SyntheticOrderFlow.QUIET_SYMBOL, Side.BUY, OrderType.MARKET,
                TimeInForce.IOC, StpMode.CANCEL_RESTING, 0, 100));
    }
}
