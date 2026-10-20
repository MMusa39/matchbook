package dev.matchbook.engine.core;

import dev.matchbook.engine.core.MatchingEnginePropertiesTest.Spec;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EngineSnapshotTest {

    @Test
    void emptyEngineRoundTrips() {
        EngineDriver original = new EngineDriver();
        EngineDriver restored = new EngineDriver();
        load(restored.engine, snapshot(original.engine));
        assertThat(snapshot(restored.engine)).isEqualTo(snapshot(original.engine));
    }

    @Test
    void restoredEngineKeepsQueueOrderCountersAndIds() {
        EngineDriver original = new EngineDriver();
        long a = EngineDriver.orderId(original.buy(1, 100, 5));
        long b = EngineDriver.orderId(original.buy(2, 100, 5));
        original.sell(3, 101, 7);
        original.sell(3, 100, 2);

        EngineDriver restored = new EngineDriver();
        load(restored.engine, snapshot(original.engine));

        assertThat(restored.engine.lastSeq()).isEqualTo(original.engine.lastSeq());
        assertThat(restored.book().bestBidQuantity()).isEqualTo(8);
        assertThat(restored.book().bestAskPrice()).isEqualTo(101);
        Command next = new Command().newOrder(5, 4, 5, EngineDriver.SYMBOL, Side.SELL, OrderType.LIMIT,
                TimeInForce.GTC, StpMode.CANCEL_RESTING, 100, 6);
        List<RecordingSink.Event> fromOriginal = original.process(next);
        List<RecordingSink.Event> fromRestored = restored.process(next);
        assertThat(fromRestored).map(Object::toString).isEqualTo(fromOriginal.stream().map(Object::toString).toList());
        assertThat(RecordingSink.trades(fromRestored)).extracting(RecordingSink.Trade::buyOrderId)
                .containsExactly(a, b);
    }

    @Test
    void rejectsLoadIntoNonEmptyEngine() {
        EngineDriver used = new EngineDriver();
        used.buy(1, 100, 1);
        byte[] bytes = snapshot(used.engine);
        assertThatThrownBy(() -> load(used.engine, bytes)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsSnapshotFromAnotherShard() {
        EngineDriver shard0 = new EngineDriver();
        EngineDriver shard1 = new EngineDriver(new EngineConfig(1, 100, List.of(EngineDriver.testSymbol(1))));
        assertThatThrownBy(() -> load(shard1.engine, snapshot(shard0.engine)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("shard");
    }

    @Test
    void rejectsCorruptSnapshot() {
        EngineDriver original = new EngineDriver();
        original.buy(1, 100, 1);
        byte[] bytes = snapshot(original.engine);
        bytes[0] ^= 0x7F;
        assertThatThrownBy(() -> load(new EngineDriver().engine, bytes)).isInstanceOf(IllegalStateException.class);
    }

    /**
     * Snapshot at an arbitrary point, restore into a fresh engine, then feed both engines the rest
     * of the commands: they must emit identical events and end in byte-identical state.
     */
    @Property(tries = 200)
    void restoreThenContinueIsIndistinguishable(@ForAll("commands") List<Spec> specs,
                                                @ForAll @IntRange(min = 0, max = 300) int cut) {
        int split = Math.min(cut, specs.size());
        EngineDriver original = new EngineDriver();
        List<Long> ids = new ArrayList<>();
        Map<Long, Integer> owners = new HashMap<>();
        Command command = new Command();
        long ts = 0;
        for (Spec s : specs.subList(0, split)) {
            MatchingEnginePropertiesTest.recordNewIds(
                    original.process(MatchingEnginePropertiesTest.toCommand(command, s, ++ts, ids, owners)), ids, owners);
        }

        EngineDriver restored = new EngineDriver();
        load(restored.engine, snapshot(original.engine));
        assertThat(snapshot(restored.engine)).isEqualTo(snapshot(original.engine));

        for (Spec s : specs.subList(split, specs.size())) {
            ts++;
            List<RecordingSink.Event> a = original.process(MatchingEnginePropertiesTest.toCommand(command, s, ts, ids, owners));
            List<RecordingSink.Event> b = restored.process(command);
            MatchingEnginePropertiesTest.recordNewIds(a, ids, owners);
            assertThat(b.stream().map(Object::toString).toList()).isEqualTo(a.stream().map(Object::toString).toList());
        }
        assertThat(snapshot(restored.engine)).isEqualTo(snapshot(original.engine));
    }

    @net.jqwik.api.Provide
    net.jqwik.api.Arbitrary<List<Spec>> commands() {
        return new MatchingEnginePropertiesTest().commands();
    }

    static byte[] snapshot(MatchingEngine engine) {
        ByteBuffer buffer = ByteBuffer.allocate(EngineSnapshot.maxLength(engine));
        int length = EngineSnapshot.write(engine, buffer, 0);
        byte[] bytes = new byte[length];
        buffer.get(0, bytes);
        return bytes;
    }

    static void load(MatchingEngine engine, byte[] bytes) {
        EngineSnapshot.read(engine, ByteBuffer.wrap(bytes), 0, bytes.length);
    }
}
