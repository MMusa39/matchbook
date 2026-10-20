package dev.matchbook.engine.pipeline;

import dev.matchbook.engine.core.Command;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * Child process for {@link KillNineRecoveryTest}: runs a pipeline with periodic snapshots and publishes
 * synthetic commands as fast as it can until it is killed. Timestamps are the command index, so the
 * parent can regenerate exactly the same commands.
 */
public final class CrashChild {

    public static void main(String[] args) {
        Path dir = Path.of(args[0]);
        long seed = Long.parseLong(args[1]);
        PipelineConfig config = PipelineConfig.defaults(SyntheticOrderFlow.engineConfig(1 << 16, true), dir)
                .withJournalSegmentBytes(1024 * 1024)
                .withSnapshotInterval(Duration.ofMillis(150));
        EnginePipeline pipeline = EnginePipeline.start(config, List.of());
        SyntheticOrderFlow flow = new SyntheticOrderFlow(seed, 4096);
        Command command = new Command();
        System.out.println("STARTED");
        System.out.flush();
        for (long i = 1; ; i++) {
            pipeline.publish(flow.next(command, i), 0, 0);
        }
    }
}
