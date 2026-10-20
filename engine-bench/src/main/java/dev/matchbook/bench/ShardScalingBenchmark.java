package dev.matchbook.bench;

import dev.matchbook.engine.core.Command;
import dev.matchbook.engine.core.EngineConfig;
import dev.matchbook.engine.pipeline.EnginePipeline;
import dev.matchbook.engine.pipeline.OutboundEvent;
import dev.matchbook.engine.pipeline.OutboundListener;
import dev.matchbook.engine.pipeline.PipelineConfig;
import dev.matchbook.engine.pipeline.SyntheticOrderFlow;
import dev.matchbook.engine.pipeline.WaitStrategyType;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * How throughput scales with the number of shards. Each shard is a complete, independent pipeline
 * (inbound ring, journal, matching, outbound ring with one consuming listener) with its own producer
 * thread publishing synthetic commands as fast as the ring accepts them (closed loop, saturation). The
 * score is matched commands per second across all shards, over the measurement window.
 *
 * <pre>
 * java -Xms4g -Xmx4g -cp engine-bench/target/benchmarks.jar dev.matchbook.bench.ShardScalingBenchmark \
 *      --shards 1,2,4,6 --runs 3 --warmup 3 --duration 10 --wait YIELDING
 * </pre>
 *
 * Each shard uses 4 busy threads (producer, journal, matching, listener) plus a background journal-segment
 * preparer and a snapshot writer; compare the thread count with the machine's cores when reading results.
 */
public final class ShardScalingBenchmark {

    /** Counts commands as their last event reaches the listener, so the outbound side is really consumed. */
    static final class CountingListener implements OutboundListener {
        volatile long commands;

        @Override
        public void onEvent(OutboundEvent event, boolean endOfBatch) {
            if (event.lastOfCommand()) {
                commands++;
            }
        }
    }

    private static int inboundRing = 1 << 14;
    private static int outboundRing = 1 << 16;

    public static void main(String[] args) throws Exception {
        List<Integer> shardCounts = List.of(1, 2, 4, 6);
        int runs = 3;
        int warmup = 3;
        int duration = 10;
        WaitStrategyType wait = WaitStrategyType.YIELDING;
        for (int i = 0; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--inbound-ring" -> inboundRing = Integer.parseInt(args[i + 1]);
                case "--outbound-ring" -> outboundRing = Integer.parseInt(args[i + 1]);
                case "--shards" -> shardCounts = Stream.of(args[i + 1].split(",")).map(Integer::parseInt).toList();
                case "--runs" -> runs = Integer.parseInt(args[i + 1]);
                case "--warmup" -> warmup = Integer.parseInt(args[i + 1]);
                case "--duration" -> duration = Integer.parseInt(args[i + 1]);
                case "--wait" -> wait = WaitStrategyType.valueOf(args[i + 1]);
                default -> throw new IllegalArgumentException(args[i]);
            }
        }
        System.out.printf("cores=%d wait=%s warmup=%ds duration=%ds inboundRing=%d outboundRing=%d%n",
                Runtime.getRuntime().availableProcessors(), wait, warmup, duration, inboundRing, outboundRing);
        for (int shards : shardCounts) {
            for (int r = 1; r <= runs; r++) {
                run(shards, r, warmup, duration, wait);
            }
        }
        System.exit(0);
    }

    private static void run(int shards, int run, int warmup, int duration, WaitStrategyType wait) throws Exception {
        Path root = Files.createTempDirectory("matchbook-shards");
        List<EnginePipeline> pipelines = new ArrayList<>();
        List<CountingListener> listeners = new ArrayList<>();
        for (int s = 0; s < shards; s++) {
            CountingListener listener = new CountingListener();
            EngineConfig engine = new EngineConfig(s, 1 << 16,
                    SyntheticOrderFlow.engineConfig(1 << 16, true).symbols());
            // A snapshot every second (keeping one) lets covered journal segments be deleted, so the run
            // measures the pipeline rather than gigabytes of journal writeback. Scheduled snapshots need a
            // multi-producer ring, which is also what the engine server uses for its gRPC threads.
            PipelineConfig config = new PipelineConfig(engine, root.resolve("shard-" + s), inboundRing, outboundRing,
                    wait, wait, true, dev.matchbook.engine.pipeline.FsyncPolicy.NONE, 256L << 20,
                    java.time.Duration.ofSeconds(1), 1, -1, -1, true, () -> Long.MAX_VALUE);
            pipelines.add(EnginePipeline.start(config, List.of(listener)));
            listeners.add(listener);
        }

        AtomicBoolean stop = new AtomicBoolean();
        CountDownLatch done = new CountDownLatch(shards);
        for (int s = 0; s < shards; s++) {
            EnginePipeline pipeline = pipelines.get(s);
            long seed = 1_000L * (s + 1) + run;
            Thread.ofPlatform().name("producer-" + s).start(() -> {
                SyntheticOrderFlow flow = new SyntheticOrderFlow(seed, 4096);
                Command command = new Command();
                long ts = 0;
                while (!stop.get()) {
                    pipeline.publish(flow.next(command, ++ts), 0, 0);
                }
                done.countDown();
            });
        }

        Thread.sleep(warmup * 1000L);
        long[] start = new long[shards];
        for (int s = 0; s < shards; s++) {
            start[s] = listeners.get(s).commands;
        }
        long cpuStart = processCpuNanos();
        long t0 = System.nanoTime();
        Thread.sleep(duration * 1000L);
        long elapsed = System.nanoTime() - t0;
        long cpu = processCpuNanos() - cpuStart;
        long total = 0;
        StringBuilder perShard = new StringBuilder();
        for (int s = 0; s < shards; s++) {
            long n = listeners.get(s).commands - start[s];
            total += n;
            perShard.append(s == 0 ? "" : ",").append(Math.round(n / (elapsed / 1e9) / 1e3)).append('k');
        }
        stop.set(true);
        done.await();
        pipelines.forEach(EnginePipeline::halt);

        double seconds = elapsed / 1e9;
        System.out.printf("shards=%d run=%d threads=%d total=%.2fM cmd/s per-shard=[%s] cpu=%.2f cores%n",
                shards, run, shards * 4, total / seconds / 1e6, perShard, cpu / 1e9 / seconds);
        deleteRecursively(root);
    }

    private static long processCpuNanos() {
        return ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
                .getProcessCpuTime();
    }

    private static void deleteRecursively(Path dir) throws IOException {
        for (int attempt = 0; attempt < 20 && Files.exists(dir); attempt++) {
            try (Stream<Path> walk = Files.walk(dir)) {
                for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(p);
                }
            } catch (IOException | java.io.UncheckedIOException e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
