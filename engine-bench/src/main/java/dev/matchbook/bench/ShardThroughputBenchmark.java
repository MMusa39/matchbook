package dev.matchbook.bench;

import dev.matchbook.engine.core.Command;
import dev.matchbook.engine.core.EngineConfig;
import dev.matchbook.engine.core.MatchingEngine;
import dev.matchbook.engine.pipeline.SyntheticOrderFlow;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;

import java.util.concurrent.TimeUnit;

/**
 * Share-nothing scaling of the matching engine: every JMH thread owns its own engine (one shard) and
 * its own synthetic order flow, exactly as shards do in the engine server. Run with {@code -t N} for
 * N shards; JMH reports the aggregate throughput across threads, so perfect scaling is N × the
 * one-thread score.
 *
 * <pre>java -jar engine-bench/target/benchmarks.jar ShardThroughputBenchmark -t 4</pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g", "-XX:+AlwaysPreTouch"})
@State(Scope.Thread)
public class ShardThroughputBenchmark {

    private MatchingEngine engine;
    private SyntheticOrderFlow flow;
    private final Command command = new Command();
    private long ts;

    @Setup(Level.Trial)
    public void setUp(Blackhole bh, ThreadParams thread) {
        int shard = thread.getThreadIndex();
        EngineConfig config = new EngineConfig(shard, 1 << 16, SyntheticOrderFlow.engineConfig(1 << 16, true).symbols());
        engine = new MatchingEngine(config, new BlackholeSink(bh));
        flow = new SyntheticOrderFlow(42, 4096); // identical work per shard, so the scores compare like for like
        for (int i = 0; i < 200_000; i++) {
            engine.process(flow.next(command, ++ts));
        }
    }

    @Benchmark
    public void mixedWorkload() {
        engine.process(flow.next(command, ++ts));
    }
}
