package dev.matchbook.bench;

import dev.matchbook.engine.core.Command;
import dev.matchbook.engine.pipeline.EnginePipeline;
import dev.matchbook.engine.pipeline.FsyncPolicy;
import dev.matchbook.engine.pipeline.OutboundEvent;
import dev.matchbook.engine.pipeline.OutboundListener;
import dev.matchbook.engine.pipeline.PipelineConfig;
import dev.matchbook.engine.pipeline.SyntheticOrderFlow;
import dev.matchbook.engine.pipeline.WaitStrategyType;
import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Open-loop latency test of the full engine pipeline (inbound ring, journal, matching, outbound ring),
 * corrected for coordinated omission.
 *
 * <p>Command {@code i} is due at {@code start + i * interval}, whether or not the system kept up. The
 * producer waits until that instant, then publishes, stamping the command with its <em>intended</em> send
 * time. The outbound listener records {@code now - intended} when the command's last event arrives. If the
 * engine stalls (GC pause, page fault, full ring), the producer falls behind its schedule, and every
 * command that should have been sent during the stall is charged the time it spent waiting, as a real
 * client sending at a fixed rate would experience. A closed-loop test, or an open-loop one that measures
 * from the actual send time, silently drops those waits: that is coordinated omission.
 *
 * <p>Both views are recorded: "response time" from the intended time (the honest number) and "service
 * time" from the actual publish time (what naive tools report), so the gap is visible.
 *
 * <pre>
 * java -jar engine-bench/target/loadgen.jar --rate 500000 --duration 30 --warmup 10 \
 *      --wait BUSY_SPIN --fsync NONE --pooling true --label busy-spin-500k
 * </pre>
 */
public final class OpenLoopLoadGenerator {

    record Options(long rate, int durationSeconds, int warmupSeconds, WaitStrategyType waitStrategy,
                   WaitStrategyType outboundWait, FsyncPolicy fsync,
                   boolean pooling, boolean pretouch, String label, Path output, Path dataDirectory) {

        static Options parse(String[] args) {
            Map<String, String> m = new LinkedHashMap<>();
            for (int i = 0; i + 1 < args.length; i += 2) {
                m.put(args[i].replaceFirst("^--", ""), args[i + 1]);
            }
            return new Options(
                    Long.parseLong(m.getOrDefault("rate", "200000")),
                    Integer.parseInt(m.getOrDefault("duration", "30")),
                    Integer.parseInt(m.getOrDefault("warmup", "10")),
                    WaitStrategyType.valueOf(m.getOrDefault("wait", "BUSY_SPIN")),
                    WaitStrategyType.valueOf(m.getOrDefault("outbound-wait", m.getOrDefault("wait", "BUSY_SPIN"))),
                    FsyncPolicy.valueOf(m.getOrDefault("fsync", "NONE")),
                    Boolean.parseBoolean(m.getOrDefault("pooling", "true")),
                    Boolean.parseBoolean(m.getOrDefault("pretouch", "true")),
                    m.getOrDefault("label", "run"),
                    Path.of(m.getOrDefault("out", "docs/results/loadgen")),
                    Path.of(m.getOrDefault("data", System.getProperty("java.io.tmpdir") + "/matchbook-loadgen")));
        }
    }

    /** Records latency of each command when its last event reaches the outbound ring's consumer. */
    static final class LatencyListener implements OutboundListener {
        final Histogram responseTime = new Histogram(10_000_000_000L, 3);
        final Histogram serviceTime = new Histogram(10_000_000_000L, 3);
        volatile long measureFromIntended = Long.MAX_VALUE;
        volatile long completed;

        @Override
        public void onEvent(OutboundEvent event, boolean endOfBatch) {
            if (!event.lastOfCommand()) {
                return;
            }
            long now = System.nanoTime();
            long intended = event.ingressNanos();
            if (intended >= measureFromIntended) {
                responseTime.recordValue(Math.max(0, now - intended));
                // The load generator puts the actual publish time in the correlation id field.
                serviceTime.recordValue(Math.max(0, now - event.correlationId()));
            }
            completed++;
        }
    }

    public static void main(String[] args) throws Exception {
        Options o = Options.parse(args);
        deleteRecursively(o.dataDirectory());
        PipelineConfig config = PipelineConfig.defaults(SyntheticOrderFlow.engineConfig(1 << 20, o.pooling()),
                        o.dataDirectory())
                .withWaitStrategies(o.waitStrategy(), o.outboundWait())
                .withProducer(false)
                .withFsync(o.fsync())
                .withJournalSegmentBytes(1L << 30)
                .withPretouchJournal(o.pretouch());
        LatencyListener listener = new LatencyListener();
        EnginePipeline pipeline = EnginePipeline.start(config, List.of(listener));

        SyntheticOrderFlow flow = new SyntheticOrderFlow(42, 4096);
        Command command = new Command();
        long interval = 1_000_000_000L / o.rate();
        long total = o.rate() * (o.warmupSeconds() + o.durationSeconds());
        long warmupCommands = o.rate() * o.warmupSeconds();

        GcSample gcBefore = null;
        long cpuBefore = 0;
        long start = System.nanoTime() + 100_000_000L;
        long measureStart = start + warmupCommands * interval;
        listener.measureFromIntended = measureStart;
        long maxBehind = 0;
        long maxPublish = 0;
        long blockedNanos = 0;
        long maxInboundDepth = 0;
        long maxOutboundDepth = 0;
        for (long i = 0; i < total; i++) {
            long intended = start + i * interval;
            if (i == warmupCommands) {
                gcBefore = GcSample.now();
                cpuBefore = processCpuNanos();
            }
            long now;
            while ((now = System.nanoTime()) < intended) {
                Thread.onSpinWait();
            }
            maxBehind = Math.max(maxBehind, now - intended);
            pipeline.publish(flow.next(command, i + 1), now, intended);
            long took = System.nanoTime() - now;
            maxPublish = Math.max(maxPublish, took);
            if (took > 10_000) {
                blockedNanos += took; // ring was full: back-pressure from the pipeline
            }
            if ((i & 1023) == 0) {
                maxInboundDepth = Math.max(maxInboundDepth, pipeline.inboundDepth());
                maxOutboundDepth = Math.max(maxOutboundDepth, pipeline.outboundDepth());
            }
        }
        long sendEnd = System.nanoTime();
        while (listener.completed < total) {
            Thread.onSpinWait();
        }
        GcSample gcAfter = GcSample.now();
        long cpuUsed = processCpuNanos() - cpuBefore;
        double measuredSeconds = (sendEnd - measureStart) / 1e9;
        pipeline.halt();

        Files.createDirectories(o.output());
        Path hgrm = o.output().resolve(o.label() + ".hgrm");
        try (PrintStream out = new PrintStream(Files.newOutputStream(hgrm))) {
            listener.responseTime.outputPercentileDistribution(out, 1000.0);
        }
        String summary = String.format(
                "label=%s rate=%d/s achieved=%.0f/s duration=%ds wait=%s/%s fsync=%s pooling=%s pretouch=%s gc=%s%n"
                        + "  response time (from intended send, CO-corrected) us: p50=%.2f p90=%.2f p99=%.2f p99.9=%.2f p99.99=%.2f max=%.2f n=%d%n"
                        + "  service time  (from actual send, naive)          us: p50=%.2f p90=%.2f p99=%.2f p99.9=%.2f p99.99=%.2f max=%.2f%n"
                        + "  producer max lag behind schedule: %.2f us; gc during measurement: %d collections, %d ms; "
                        + "process cpu: %.2f cores%n"
                        + "  publish: max %.2f us, blocked on full ring %.1f ms total; max ring depth inbound %d, outbound %d%n",
                o.label(), o.rate(), listener.responseTime.getTotalCount() / measuredSeconds, o.durationSeconds(),
                o.waitStrategy(), o.outboundWait(), o.fsync(), o.pooling(), o.pretouch(), gcNames(),
                us(listener.responseTime, 50), us(listener.responseTime, 90), us(listener.responseTime, 99),
                us(listener.responseTime, 99.9), us(listener.responseTime, 99.99),
                listener.responseTime.getMaxValue() / 1000.0, listener.responseTime.getTotalCount(),
                us(listener.serviceTime, 50), us(listener.serviceTime, 90), us(listener.serviceTime, 99),
                us(listener.serviceTime, 99.9), us(listener.serviceTime, 99.99),
                listener.serviceTime.getMaxValue() / 1000.0,
                maxBehind / 1000.0, gcAfter.count - gcBefore.count, gcAfter.millis - gcBefore.millis,
                cpuUsed / 1e9 / measuredSeconds,
                maxPublish / 1000.0, blockedNanos / 1e6, maxInboundDepth, maxOutboundDepth);
        System.out.print(summary);
        Files.writeString(o.output().resolve(o.label() + ".txt"), summary);
        deleteRecursively(o.dataDirectory());
        System.exit(0);
    }

    private static double us(Histogram h, double percentile) {
        return h.getValueAtPercentile(percentile) / 1000.0;
    }

    record GcSample(long count, long millis) {
        static GcSample now() {
            long count = 0;
            long millis = 0;
            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                count += Math.max(0, gc.getCollectionCount());
                millis += Math.max(0, gc.getCollectionTime());
            }
            return new GcSample(count, millis);
        }
    }

    private static String gcNames() {
        return String.join("+", ManagementFactory.getGarbageCollectorMXBeans().stream()
                .map(GarbageCollectorMXBean::getName).map(n -> n.replace(' ', '_')).toList());
    }

    private static long processCpuNanos() {
        return ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
                .getProcessCpuTime();
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
    }
}
