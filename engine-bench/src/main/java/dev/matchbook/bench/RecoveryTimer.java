package dev.matchbook.bench;

import dev.matchbook.engine.core.Command;
import dev.matchbook.engine.pipeline.EnginePipeline;
import dev.matchbook.engine.pipeline.PipelineConfig;
import dev.matchbook.engine.pipeline.SyntheticOrderFlow;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Measures restart time: builds a journal of {@code --commands} synthetic commands, kills the pipeline
 * without a final snapshot, then times {@link EnginePipeline#start} (snapshot load + journal replay) over
 * several runs. With {@code --snapshot-at} a snapshot is taken after that many commands first.
 * The journal files are in the OS page cache during the timed runs (warm restart).
 */
public final class RecoveryTimer {

    public static void main(String[] args) throws Exception {
        long commands = 10_000_000;
        long snapshotAt = 0;
        int runs = 5;
        for (int i = 0; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--commands" -> commands = Long.parseLong(args[i + 1]);
                case "--snapshot-at" -> snapshotAt = Long.parseLong(args[i + 1]);
                case "--runs" -> runs = Integer.parseInt(args[i + 1]);
                default -> throw new IllegalArgumentException(args[i]);
            }
        }
        Path dir = Files.createTempDirectory("matchbook-recovery");
        PipelineConfig config = PipelineConfig.defaults(SyntheticOrderFlow.engineConfig(1 << 20, true), dir)
                .withJournalSegmentBytes(1L << 30);

        EnginePipeline writer = EnginePipeline.start(config, List.of());
        SyntheticOrderFlow flow = new SyntheticOrderFlow(42, 4096);
        Command command = new Command();
        for (long i = 1; i <= commands; i++) {
            writer.publish(flow.next(command, i), 0, 0);
            if (i == snapshotAt) {
                writer.snapshot(true).get(60, TimeUnit.SECONDS);
            }
        }
        writer.snapshot(false).get(60, TimeUnit.SECONDS); // barrier: everything journaled and matched
        writer.halt();

        List<Long> millis = new ArrayList<>();
        EnginePipeline.RecoveryInfo info = null;
        for (int r = 0; r < runs; r++) {
            long start = System.nanoTime();
            EnginePipeline pipeline = EnginePipeline.start(config, List.of());
            long elapsed = System.nanoTime() - start;
            info = pipeline.recoveryInfo();
            pipeline.halt();
            millis.add(TimeUnit.NANOSECONDS.toMillis(elapsed));
        }
        List<Long> sorted = millis.stream().sorted().toList();
        System.out.printf("commands=%d snapshotAt=%d replayed=%d journalBytesUsed=%d runs=%s median=%d ms "
                        + "(start() total; replay only %.0f ms on last run => %.1f M commands/s)%n",
                commands, info.snapshotJournalSeq(), info.replayedCommands(), info.lastJournalSeq() * 80, millis,
                sorted.get(sorted.size() / 2), info.recoveryNanos() / 1e6,
                info.replayedCommands() / (info.recoveryNanos() / 1e9) / 1e6);
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
        System.exit(0);
    }
}
