package dev.matchbook.marketdata;

import dev.matchbook.engine.server.EngineServer;
import dev.matchbook.engine.server.EngineServerConfig;
import dev.matchbook.proto.v1.EngineCommand;
import dev.matchbook.proto.v1.MatchingEngineGrpc;
import dev.matchbook.proto.v1.NewOrder;
import dev.matchbook.proto.v1.OrderType;
import dev.matchbook.proto.v1.Side;
import dev.matchbook.proto.v1.StpMode;
import dev.matchbook.proto.v1.TimeInForce;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** End to end: engine → gRPC market data → replicas → WebSocket client. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MarketDataIT {

    static EngineServer engine;
    static ManagedChannel channel;
    static MatchingEngineGrpc.MatchingEngineBlockingStub orders;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put("ENGINE_GRPC_PORT", "0");
        env.put("ENGINE_METRICS_PORT", "0");
        env.put("ENGINE_DATA_DIR", Files.createTempDirectory("md-it-engine").toString());
        env.put("ENGINE_MAX_ORDERS_PER_SHARD", "10000");
        env.put("ENGINE_SNAPSHOT_INTERVAL", "PT0S");
        engine = new EngineServer(EngineServerConfig.fromMap(env)).start();
        channel = ManagedChannelBuilder.forAddress("localhost", engine.port()).usePlaintext().build();
        orders = MatchingEngineGrpc.newBlockingStub(channel);
        registry.add("matchbook.engine-target", () -> "localhost:" + engine.port());
    }

    @AfterAll
    static void stopEngine() {
        channel.shutdownNow();
        engine.close();
    }

    @LocalServerPort
    int port;

    @Autowired
    EngineFeed feed;

    @Autowired
    JsonMapper json;

    static void order(long client, Side side, long price, long qty) {
        orders.withDeadlineAfter(5, TimeUnit.SECONDS).submit(EngineCommand.newBuilder().setNewOrder(NewOrder.newBuilder()
                .setClientId(client).setClOrdId(System.nanoTime()).setSymbolId(1).setSide(side)
                .setOrderType(OrderType.ORDER_TYPE_LIMIT).setTimeInForce(TimeInForce.TIME_IN_FORCE_GTC)
                .setStpMode(StpMode.STP_MODE_CANCEL_RESTING).setPrice(price).setQuantity(qty)).build());
    }

    @Test
    void subscriberGetsASnapshotThenContiguousIncrementals() throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!feed.isConnected() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        order(1, Side.SIDE_BUY, 15_000, 10);
        order(1, Side.SIDE_SELL, 15_100, 4);
        Thread.sleep(300);

        BlockingQueue<JsonNode> received = new LinkedBlockingQueue<>();
        WebSocketSession ws = new StandardWebSocketClient().execute(new TextWebSocketHandler() {
            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                received.add(json.readTree(message.getPayload()));
            }
        }, "ws://localhost:" + port + "/ws/market-data").get(5, TimeUnit.SECONDS);
        ws.sendMessage(new TextMessage("{\"op\":\"subscribe\",\"symbols\":[\"AAPL\"]}"));

        JsonNode snapshot = received.poll(5, TimeUnit.SECONDS);
        assertThat(snapshot.path("type").asString()).isEqualTo("snapshot");
        assertThat(snapshot.path("bids").get(0).path("price").asString()).isEqualTo("150.00");
        assertThat(snapshot.path("bids").get(0).path("quantity").asLong()).isEqualTo(10);
        assertThat(snapshot.path("asks").get(0).path("price").asString()).isEqualTo("151.00");
        long seq = snapshot.path("seq").asLong();

        order(2, Side.SIDE_BUY, 15_100, 4); // trade 4@151.00, ask level removed, top changes
        String[] expectedTypes = {"trade", "level", "top"};
        for (String type : expectedTypes) {
            JsonNode m = received.poll(5, TimeUnit.SECONDS);
            assertThat(m).as("expected a %s message", type).isNotNull();
            assertThat(m.path("type").asString()).isEqualTo(type);
            assertThat(m.path("seq").asLong()).isEqualTo(++seq);
        }
        ws.close();
    }
}
