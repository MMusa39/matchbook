package dev.matchbook.bench;

import dev.matchbook.engine.core.Command;
import dev.matchbook.engine.core.EngineConfig;
import dev.matchbook.engine.core.OrderType;
import dev.matchbook.engine.core.Side;
import dev.matchbook.engine.core.StpMode;
import dev.matchbook.engine.core.SymbolConfig;
import dev.matchbook.engine.core.TimeInForce;
import dev.matchbook.engine.pipeline.EnginePipeline;
import dev.matchbook.engine.pipeline.PipelineConfig;
import dev.matchbook.engine.pipeline.Snapshot;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Measures how long a snapshot pauses the matching thread (serialising the engine into memory; the file
 * write happens on another thread), for books of 10k, 100k and 1M resting orders. Each size is measured
 * {@code runs} times after warm-up snapshots.
 */
public final class SnapshotPauseTimer {

    public static void main(String[] args) throws Exception {
        int runs = args.length > 0 ? Integer.parseInt(args[0]) : 7;
        for (int resting : new int[]{10_000, 100_000, 1_000_000}) {
            measure(resting, runs);
        }
        System.exit(0);
    }

    private static void measure(int resting, int runs) throws Exception {
        Path dir = Files.createTempDirectory("matchbook-snapshot");
        SymbolConfig symbol = new SymbolConfig(1, "SNAP", 100, 1, 1, 1, 1_000_000, 1, 200_000);
        PipelineConfig config = PipelineConfig.defaults(new EngineConfig(0, resting + 16, List.of(symbol)), dir)
                .withJournalSegmentBytes(1L << 30);
        EnginePipeline pipeline = EnginePipeline.start(config, List.of());
        Command command = new Command();
        for (int i = 0; i < resting; i++) {
            // Non-crossing bids and asks spread over many levels, several orders per level.
            boolean buy = (i & 1) == 0;
            long price = buy ? 100_000 - (i % 5_000) - 1 : 100_000 + (i % 5_000) + 1;
            pipeline.publish(command.newOrder(i + 1, 1 + (i % 50), i + 1, 1, buy ? Side.BUY : Side.SELL,
                    OrderType.LIMIT, TimeInForce.GTC, StpMode.CANCEL_RESTING, price, 10), 0, 0);
        }
        for (int w = 0; w < 3; w++) {
            pipeline.snapshot(false).get(60, TimeUnit.SECONDS);
        }
        List<Double> pauses = new ArrayList<>();
        int bytes = 0;
        for (int r = 0; r < runs; r++) {
            Snapshot s = pipeline.snapshot(false).get(60, TimeUnit.SECONDS);
            pauses.add(s.pauseNanos() / 1e6);
            bytes = s.state().length;
        }
        pipeline.halt();
        List<Double> sorted = pauses.stream().sorted().toList();
        System.out.printf("resting=%d snapshotBytes=%d pause ms: median=%.2f min=%.2f max=%.2f all=%s%n", resting, bytes,
                sorted.get(sorted.size() / 2), sorted.getFirst(), sorted.getLast(),
                pauses.stream().map(p -> String.format("%.2f", p)).toList());
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        }
    }
}
