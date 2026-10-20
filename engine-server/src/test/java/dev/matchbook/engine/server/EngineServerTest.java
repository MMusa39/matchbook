package dev.matchbook.engine.server;

import dev.matchbook.proto.v1.EngineCommand;
import dev.matchbook.proto.v1.EngineEvent;
import dev.matchbook.proto.v1.EngineResult;
import dev.matchbook.proto.v1.ExecType;
import dev.matchbook.proto.v1.MarketDataEvent;
import dev.matchbook.proto.v1.MarketDataRequest;
import dev.matchbook.proto.v1.MatchingEngineGrpc;
import dev.matchbook.proto.v1.NewOrder;
import dev.matchbook.proto.v1.OrderType;
import dev.matchbook.proto.v1.Side;
import dev.matchbook.proto.v1.StpMode;
import dev.matchbook.proto.v1.StreamEventsRequest;
import dev.matchbook.proto.v1.TimeInForce;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EngineServerTest {

    static final int AAPL = 1;

    @TempDir
    Path dir;

    private EngineServer server;
    private ManagedChannel channel;
    private String name;

    static EngineServerConfig config(Path dir, String kafka) {
        Map<String, String> env = new HashMap<>();
        env.put("ENGINE_DATA_DIR", dir.toString());
        env.put("ENGINE_METRICS_PORT", "0");
        env.put("ENGINE_MAX_ORDERS_PER_SHARD", "10000");
        env.put("ENGINE_SNAPSHOT_INTERVAL", "PT0S");
        env.put("KAFKA_BOOTSTRAP_SERVERS", kafka);
        return EngineServerConfig.fromMap(env);
    }

    MatchingEngineGrpc.MatchingEngineBlockingStub start() {
        name = InProcessServerBuilder.generateName();
        server = new EngineServer(config(dir, ""), InProcessServerBuilder.forName(name).directExecutor()).start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        return MatchingEngineGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
    }

    @AfterEach
    void stop() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.close();
        }
    }

    static EngineCommand order(long client, long clOrdId, Side side, long price, long quantity) {
        return EngineCommand.newBuilder().setNewOrder(NewOrder.newBuilder()
                .setClientId(client).setClOrdId(clOrdId).setSymbolId(AAPL).setSide(side)
                .setOrderType(OrderType.ORDER_TYPE_LIMIT).setTimeInForce(TimeInForce.TIME_IN_FORCE_GTC)
                .setStpMode(StpMode.STP_MODE_CANCEL_RESTING).setPrice(price).setQuantity(quantity)).build();
    }

    @Test
    void submitReturnsTheCommandsOwnReportsAndTrades() {
        var engine = start();
        EngineResult resting = engine.submit(order(1, 1, Side.SIDE_SELL, 15_000, 10));
        assertThat(resting.getReportsList()).extracting(r -> r.getExecType()).containsExactly(ExecType.EXEC_TYPE_NEW);
        long restingId = resting.getReports(0).getOrderId();

        EngineResult aggressive = engine.submit(order(2, 1, Side.SIDE_BUY, 15_010, 4));
        assertThat(aggressive.getReportsList()).extracting(r -> r.getExecType())
                .containsExactly(ExecType.EXEC_TYPE_NEW, ExecType.EXEC_TYPE_FILL);
        assertThat(aggressive.getReportsList()).allMatch(r -> r.getClientId() == 2);
        assertThat(aggressive.getTradesList()).singleElement().satisfies(t -> {
            assertThat(t.getPrice()).isEqualTo(15_000);
            assertThat(t.getQuantity()).isEqualTo(4);
            assertThat(t.getSellOrderId()).isEqualTo(restingId);
        });
        assertThat(aggressive.getJournalSeq()).isEqualTo(2);
    }

    @Test
    void engineRejectsAreReturnedNotThrown() {
        var engine = start();
        EngineResult result = engine.submit(order(1, 1, Side.SIDE_BUY, 1, 10)); // below the band
        assertThat(result.getReports(0).getExecType()).isEqualTo(ExecType.EXEC_TYPE_REJECTED);
        assertThat(result.getReports(0).getReason()).isEqualTo("PRICE_OUT_OF_BAND");
    }

    @Test
    void unknownSymbolIsInvalidArgument() {
        var engine = start();
        EngineCommand command = order(1, 1, Side.SIDE_BUY, 15_000, 1).toBuilder()
                .setNewOrder(order(1, 1, Side.SIDE_BUY, 15_000, 1).getNewOrder().toBuilder().setSymbolId(99)).build();
        assertThatThrownBy(() -> engine.submit(command)).isInstanceOf(StatusRuntimeException.class)
                .extracting(e -> ((StatusRuntimeException) e).getStatus().getCode())
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    @Test
    void eventStreamCarriesFillsOfOtherClientsRestingOrders() throws Exception {
        var engine = start();
        BlockingQueue<EngineEvent> events = new LinkedBlockingQueue<>();
        MatchingEngineGrpc.newStub(channel).streamEvents(StreamEventsRequest.newBuilder().setSubscriber("t").build(),
                collector(events));
        waitForSubscribers(() -> server.eventSubscribers().size(), 1);

        engine.submit(order(1, 1, Side.SIDE_SELL, 15_000, 10));
        engine.submit(order(2, 1, Side.SIDE_BUY, 15_000, 10));

        List<EngineEvent> seen = drain(events, 5);
        assertThat(seen).anySatisfy(e -> {
            assertThat(e.hasExecutionReport()).isTrue();
            assertThat(e.getExecutionReport().getClientId()).isEqualTo(1);
            assertThat(e.getExecutionReport().getExecType()).isEqualTo(ExecType.EXEC_TYPE_FILL);
        });
        assertThat(seen).anyMatch(EngineEvent::hasTrade);
    }

    @Test
    void marketDataStartsWithAnImageThenContiguousIncrementals() throws Exception {
        var engine = start();
        engine.submit(order(1, 1, Side.SIDE_BUY, 14_990, 5));
        engine.submit(order(1, 2, Side.SIDE_SELL, 15_010, 7));

        BlockingQueue<MarketDataEvent> events = new LinkedBlockingQueue<>();
        MatchingEngineGrpc.newStub(channel).streamMarketData(MarketDataRequest.newBuilder().setSubscriber("t").build(),
                collector(events));
        MarketDataEvent first = poll(events, AAPL);
        assertThat(first.hasImage()).isTrue();
        assertThat(first.getImage().getBidsList()).singleElement().satisfies(l -> assertThat(l.getQuantity()).isEqualTo(5));
        assertThat(first.getImage().getAsksList()).singleElement().satisfies(l -> assertThat(l.getPrice()).isEqualTo(15_010));
        long seq = first.getImage().getSymbolSeq();

        engine.submit(order(2, 1, Side.SIDE_BUY, 15_010, 7));
        for (int i = 0; i < 3; i++) { // trade, level, top
            MarketDataEvent next = poll(events, AAPL);
            long nextSeq = next.hasTrade() ? next.getTrade().getSymbolSeq()
                    : next.hasLevel() ? next.getLevel().getSymbolSeq() : next.getTop().getSymbolSeq();
            assertThat(nextSeq).isEqualTo(++seq);
        }
    }

    @Test
    void stateSurvivesARestart() {
        var engine = start();
        long id = engine.submit(order(1, 1, Side.SIDE_BUY, 15_000, 10)).getReports(0).getOrderId();
        stop();
        channel = null;

        engine = start();
        EngineResult cancelled = engine.submit(EngineCommand.newBuilder().setCancel(
                dev.matchbook.proto.v1.Cancel.newBuilder().setClientId(1).setClOrdId(2).setSymbolId(AAPL)
                        .setOrderId(id)).build());
        assertThat(cancelled.getReports(0).getExecType()).isEqualTo(ExecType.EXEC_TYPE_CANCELED);
    }

    @Test
    void exposesPrometheusMetrics() {
        var engine = start();
        engine.submit(order(1, 1, Side.SIDE_BUY, 15_000, 10));
        engine.submit(order(1, 2, Side.SIDE_BUY, 1, 10));
        String scrape = awaitScrapeContaining("matchbook_engine_rejects_total");
        assertThat(scrape).contains("matchbook_engine_commands_total{result=\"NEW\",shard=\"0\"}");
        assertThat(scrape).contains("matchbook_engine_rejects_total{reason=\"PRICE_OUT_OF_BAND\",shard=\"0\"}");
        assertThat(scrape).contains("matchbook_engine_latency_seconds_bucket");
        assertThat(scrape).contains("matchbook_engine_match_seconds_bucket");
        assertThat(scrape).contains("matchbook_engine_queue_depth{ring=\"inbound\",shard=\"0\"}");
    }

    private String awaitScrapeContaining(String text) {
        long deadline = System.currentTimeMillis() + 5_000;
        String scrape = server.registry().scrape();
        while (!scrape.contains(text) && System.currentTimeMillis() < deadline) {
            Thread.onSpinWait();
            scrape = server.registry().scrape();
        }
        return scrape;
    }

    static <T> StreamObserver<T> collector(BlockingQueue<T> queue) {
        return new StreamObserver<>() {
            @Override
            public void onNext(T value) {
                queue.add(value);
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
            }
        };
    }

    private static MarketDataEvent poll(BlockingQueue<MarketDataEvent> events, int symbol) throws InterruptedException {
        while (true) {
            MarketDataEvent e = events.poll(5, TimeUnit.SECONDS);
            assertThat(e).as("market data event").isNotNull();
            int id = e.hasImage() ? e.getImage().getSymbolId() : e.hasLevel() ? e.getLevel().getSymbolId()
                    : e.hasTop() ? e.getTop().getSymbolId() : e.getTrade().getSymbolId();
            if (id == symbol) {
                return e;
            }
        }
    }

    private static <T> List<T> drain(BlockingQueue<T> queue, int atLeast) throws InterruptedException {
        java.util.ArrayList<T> out = new java.util.ArrayList<>();
        while (out.size() < atLeast) {
            T e = queue.poll(5, TimeUnit.SECONDS);
            assertThat(e).isNotNull();
            out.add(e);
        }
        queue.drainTo(out);
        return out;
    }

    private static void waitForSubscribers(java.util.function.IntSupplier count, int expected) {
        long deadline = System.currentTimeMillis() + 5_000;
        while (count.getAsInt() < expected && System.currentTimeMillis() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(count.getAsInt()).isGreaterThanOrEqualTo(expected);
    }
}
