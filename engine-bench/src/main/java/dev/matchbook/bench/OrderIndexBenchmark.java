package dev.matchbook.bench;

import org.agrona.collections.Long2ObjectHashMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * Order-id index choice in isolation: a put, a get and a remove per operation against a map holding
 * {@code live} entries, keyed by sequential ids as the engine assigns them. Compares Agrona's
 * open-addressing {@link Long2ObjectHashMap} with {@link HashMap}{@code <Long, Object>}, which boxes keys
 * and allocates a node per entry. Run with {@code -prof gc}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 3, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Thread)
public class OrderIndexBenchmark {

    @Param({"4096", "1000000"})
    public int live;

    private final Object value = new Object();
    private Long2ObjectHashMap<Object> agrona;
    private HashMap<Long, Object> jdk;
    private long next;

    @Setup(Level.Trial)
    public void setUp() {
        agrona = new Long2ObjectHashMap<>(live * 2, 0.6f);
        jdk = new HashMap<>(live * 2, 0.75f);
        for (long id = 1; id <= live; id++) {
            agrona.put(id, value);
            jdk.put(id, value);
        }
        next = live + 1;
    }

    @Benchmark
    public Object agronaLong2Object() {
        long id = next++;
        agrona.put(id, value);
        Object found = agrona.get(id - live / 2);
        agrona.remove(id - live);
        return found;
    }

    @Benchmark
    public Object jdkHashMapBoxed() {
        long id = next++;
        jdk.put(id, value);
        Object found = jdk.get(id - live / 2);
        jdk.remove(id - live);
        return found;
    }
}
