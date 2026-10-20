package dev.matchbook.engine.pipeline;

import dev.matchbook.engine.core.Command;
import dev.matchbook.engine.core.EventSink;
import dev.matchbook.engine.core.MatchingEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The strongest recovery check: a separate JVM runs the pipeline under load and is killed with SIGKILL
 * (no shutdown hooks, no final snapshot, journal mid-write). Recovery from whatever it left on disk must
 * reproduce, byte for byte, the state of a fresh engine fed exactly the commands that reached the journal.
 */
class KillNineRecoveryTest {

    @TempDir
    Path dir;

    @Test
    void recoversExactStateAfterSigkill() throws Exception {
        long seed = 99;
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process child = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                CrashChild.class.getName(), dir.toString(), Long.toString(seed))
                .redirectErrorStream(true)
                .start();
        try (BufferedReader out = new BufferedReader(new InputStreamReader(child.getInputStream()))) {
            String line;
            while ((line = out.readLine()) != null && !line.equals("STARTED")) {
                // skip log lines
            }
            assertThat(line).isEqualTo("STARTED");
            Thread.sleep(1_500);
            child.destroyForcibly(); // SIGKILL on Unix
            assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue();
        }

        EnginePipeline.RecoveryInfo info;
        byte[] recovered;
        try (EnginePipeline pipeline = EnginePipeline.start(
                PipelineConfig.defaults(SyntheticOrderFlow.engineConfig(1 << 16, true), dir), List.of())) {
            info = pipeline.recoveryInfo();
            recovered = pipeline.snapshot(false).get(30, TimeUnit.SECONDS).state();
        }

        assertThat(info.snapshotJournalSeq()).as("a periodic snapshot was taken before the kill").isPositive();
        assertThat(info.lastJournalSeq()).isGreaterThan(info.snapshotJournalSeq());

        MatchingEngine reference = new MatchingEngine(SyntheticOrderFlow.engineConfig(1 << 16, true),
                EventSink.NO_OP);
        SyntheticOrderFlow flow = new SyntheticOrderFlow(seed, 4096);
        Command command = new Command();
        for (long i = 1; i <= info.lastJournalSeq(); i++) {
            reference.process(flow.next(command, i));
        }
        assertThat(recovered).isEqualTo(EnginePipelineTest.state(reference));
        System.out.printf("kill -9 recovery: journal=%d commands, snapshot at %d, replayed %d in %.1f ms%n",
                info.lastJournalSeq(), info.snapshotJournalSeq(), info.replayedCommands(),
                info.recoveryNanos() / 1e6);
    }

}
