package dev.matchbook.engine.pipeline;

import dev.matchbook.engine.core.Command;
import dev.matchbook.engine.core.EngineConfig;
import dev.matchbook.engine.core.EngineSnapshot;
import dev.matchbook.engine.core.MatchingEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class EnginePipelineTest {

    private static final EngineConfig ENGINE = SyntheticOrderFlow.engineConfig(1 << 16, true);

    @TempDir
    Path dir;

    /** Runs {@code count} synthetic commands through a bare engine and returns its events and final state. */
    static Reference reference(long seed, int count) {
        StringSink sink = new StringSink();
        MatchingEngine engine = new MatchingEngine(ENGINE, sink);
        SyntheticOrderFlow flow = new SyntheticOrderFlow(seed, 4096);
        Command command = new Command();
        for (int i = 1; i <= count; i++) {
            engine.process(flow.next(command, i));
        }
        return new Reference(sink.events, state(engine));
    }

    record Reference(List<String> events, byte[] state) {
    }

    static byte[] state(MatchingEngine engine) {
        ByteBuffer buffer = ByteBuffer.allocate(EngineSnapshot.maxLength(engine));
        int length = EngineSnapshot.write(engine, buffer, 0);
        byte[] bytes = new byte[length];
        buffer.get(0, bytes);
        return bytes;
    }

    static void publish(EnginePipeline pipeline, SyntheticOrderFlow flow, int from, int to) {
        Command command = new Command();
        for (int i = from; i <= to; i++) {
            pipeline.publish(flow.next(command, i), i, System.nanoTime());
        }
    }

    @ParameterizedTest
    @EnumSource(WaitStrategyType.class)
    void deliversExactlyTheEngineOutputUnderEveryWaitStrategy(WaitStrategyType wait) throws Exception {
        Reference expected = reference(11, 20_000);
        RecordingListener listener = new RecordingListener();
        try (EnginePipeline pipeline = EnginePipeline.start(
                PipelineConfig.defaults(ENGINE, dir).withWaitStrategies(wait, wait), List.of(listener))) {
            publish(pipeline, new SyntheticOrderFlow(11, 4096), 1, 20_000);
            List<RecordingListener.Seen> seen = listener.awaitAtLeast(expected.events().size(), 30, TimeUnit.SECONDS);

            assertThat(seen).extracting(RecordingListener.Seen::text).isEqualTo(expected.events());
            assertThat(seen).filteredOn(RecordingListener.Seen::lastOfCommand).hasSize(20_000);
            assertThat(pipeline.lastJournalSeq()).isEqualTo(20_000);
            assertThat(pipeline.snapshot(false).get(10, TimeUnit.SECONDS).state()).isEqualTo(expected.state());
        }
    }

    @Test
    void everyEventCarriesItsCommandsCorrelationIdAndJournalSeq() throws Exception {
        Reference expected = reference(3, 2_000);
        RecordingListener listener = new RecordingListener();
        try (EnginePipeline pipeline = EnginePipeline.start(PipelineConfig.defaults(ENGINE, dir), List.of(listener))) {
            publish(pipeline, new SyntheticOrderFlow(3, 4096), 1, 2_000);
            List<RecordingListener.Seen> seen = listener.awaitAtLeast(expected.events().size(), 30, TimeUnit.SECONDS);

            long command = 1;
            for (RecordingListener.Seen s : seen) {
                assertThat(s.correlationId()).isEqualTo(command);
                assertThat(s.journalSeq()).isEqualTo(command);
                if (s.lastOfCommand()) {
                    command++;
                }
            }
            assertThat(command).isEqualTo(2_001);
        }
    }

    @Test
    void recoversIdenticalStateAfterAHaltUsingSnapshotPlusJournal() throws Exception {
        RecordingListener before = new RecordingListener();
        SyntheticOrderFlow flow = new SyntheticOrderFlow(5, 4096);
        EnginePipeline first = EnginePipeline.start(PipelineConfig.defaults(ENGINE, dir), List.of(before));
        publish(first, flow, 1, 30_000);
        Snapshot persisted = first.snapshot(true).get(10, TimeUnit.SECONDS);
        publish(first, flow, 30_001, 50_000);
        byte[] preCrash = first.snapshot(false).get(10, TimeUnit.SECONDS).state();
        List<RecordingListener.Seen> beforeEvents = before.awaitAtLeast(1, 1, TimeUnit.SECONDS);
        first.halt();
        assertThat(persisted.journalSeq()).isEqualTo(30_000);

        RecordingListener replayed = new RecordingListener();
        try (EnginePipeline second = EnginePipeline.start(PipelineConfig.defaults(ENGINE, dir), List.of(replayed))) {
            EnginePipeline.RecoveryInfo info = second.recoveryInfo();
            assertThat(info.snapshotJournalSeq()).isEqualTo(30_000);
            assertThat(info.replayedCommands()).isEqualTo(20_000);
            assertThat(info.lastJournalSeq()).isEqualTo(50_000);
            assertThat(second.snapshot(false).get(10, TimeUnit.SECONDS).state())
                    .as("recovered state is byte-identical to the pre-crash state").isEqualTo(preCrash);

            // The replay re-emits exactly the events originally produced after the snapshot.
            List<String> originalTail = before.snapshot().stream()
                    .filter(s -> s.journalSeq() > 30_000).map(RecordingListener.Seen::text).toList();
            List<RecordingListener.Seen> replay = replayed.awaitAtLeast(originalTail.size(), 30, TimeUnit.SECONDS);
            assertThat(replay.subList(0, originalTail.size())).extracting(RecordingListener.Seen::text)
                    .isEqualTo(originalTail);
            assertThat(replay).allMatch(s -> s.correlationId() == 0);

            // And carries on exactly as an engine that never crashed.
            publish(second, flow, 50_001, 60_000);
            assertThat(second.snapshot(false).get(10, TimeUnit.SECONDS).state())
                    .isEqualTo(reference(5, 60_000).state());
        }
        assertThat(beforeEvents).isNotEmpty();
    }

    @Test
    void cleanShutdownWritesAFinalSnapshotSoRestartReplaysNothing() throws Exception {
        SyntheticOrderFlow flow = new SyntheticOrderFlow(9, 4096);
        try (EnginePipeline pipeline = EnginePipeline.start(PipelineConfig.defaults(ENGINE, dir), List.of())) {
            publish(pipeline, flow, 1, 5_000);
        }
        try (EnginePipeline pipeline = EnginePipeline.start(PipelineConfig.defaults(ENGINE, dir), List.of())) {
            assertThat(pipeline.recoveryInfo().snapshotJournalSeq()).isEqualTo(5_000);
            assertThat(pipeline.recoveryInfo().replayedCommands()).isZero();
            assertThat(pipeline.snapshot(false).get(10, TimeUnit.SECONDS).state())
                    .isEqualTo(reference(9, 5_000).state());
        }
    }

    @Test
    void recoversFromJournalAloneWhenThereIsNoSnapshot() throws Exception {
        EnginePipeline first = EnginePipeline.start(PipelineConfig.defaults(ENGINE, dir), List.of());
        publish(first, new SyntheticOrderFlow(13, 4096), 1, 8_000);
        first.snapshot(false).get(10, TimeUnit.SECONDS);
        first.halt();

        try (EnginePipeline second = EnginePipeline.start(PipelineConfig.defaults(ENGINE, dir), List.of())) {
            assertThat(second.recoveryInfo().snapshotJournalSeq()).isZero();
            assertThat(second.recoveryInfo().replayedCommands()).isEqualTo(8_000);
            assertThat(second.snapshot(false).get(10, TimeUnit.SECONDS).state())
                    .isEqualTo(reference(13, 8_000).state());
        }
    }

    @Test
    void bookImageIsConsistentWithTheEventStream() throws Exception {
        RecordingListener listener = new RecordingListener();
        try (EnginePipeline pipeline = EnginePipeline.start(PipelineConfig.defaults(ENGINE, dir), List.of(listener))) {
            publish(pipeline, new SyntheticOrderFlow(17, 4096), 1, 3_000);
            pipeline.requestBookImage(SyntheticOrderFlow.SYMBOL);
            // The image is emitted on the matching thread; the listener consumes it on its own thread.
            List<RecordingListener.Seen> seen = listener.awaitText("IMAGE_END", 10, TimeUnit.SECONDS);
            int start = indexOf(seen, "IMAGE_START");
            int end = indexOf(seen, "IMAGE_END");
            assertThat(start).isPositive();
            assertThat(end).isGreaterThan(start + 1);
            assertThat(seen.get(end).lastOfCommand()).isTrue();
            assertThat(seen.subList(start + 1, end)).allMatch(s -> s.text().startsWith("IMAGE_LEVEL"));
        }
    }

    private static int indexOf(List<RecordingListener.Seen> seen, String prefix) {
        for (int i = 0; i < seen.size(); i++) {
            if (seen.get(i).text().startsWith(prefix)) {
                return i;
            }
        }
        return -1;
    }
}
