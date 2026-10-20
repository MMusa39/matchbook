package dev.matchbook.gateway;

import dev.matchbook.proto.v1.ClientExecutionReport;
import dev.matchbook.proto.v1.ExecType;
import dev.matchbook.proto.v1.OrderEntryGrpc;
import dev.matchbook.proto.v1.OrderResponse;
import dev.matchbook.proto.v1.OrderType;
import dev.matchbook.proto.v1.Side;
import dev.matchbook.proto.v1.StreamExecutionsRequest;
import dev.matchbook.proto.v1.SubmitOrderRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.grpc.server.lifecycle.GrpcServerLifecycle;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End to end through the gateway: real Redis, real engine, REST and gRPC. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "matchbook.rate-limit.capacity=1000",
        "matchbook.rate-limit.refill-per-second=1000"})
class GatewayIT {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        TestEnvironment.register(registry);
    }

    @LocalServerPort
    int httpPort;

    @Autowired
    GrpcServerLifecycle grpcServer;

    private ManagedChannel channel;

    @AfterEach
    void closeChannel() {
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    RestClient rest(String apiKey) {
        RestClient.Builder builder = RestClient.builder().baseUrl("http://localhost:" + httpPort)
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .defaultStatusHandler(status -> true, (req, res) -> { });
        if (apiKey != null) {
            builder.defaultHeader("X-API-Key", apiKey);
        }
        return builder.build();
    }

    record Response(int status, Map<String, Object> body, HttpHeaders headers) {
    }

    @SuppressWarnings("unchecked")
    Response post(String apiKey, String path, Map<String, Object> body) {
        var entity = rest(apiKey).post().uri(path).body(body).retrieve().toEntity(Map.class);
        return new Response(entity.getStatusCode().value(), entity.getBody(), entity.getHeaders());
    }

    static String id() {
        return UUID.randomUUID().toString();
    }

    static Map<String, Object> limit(String clOrdId, String side, String price, String qty) {
        return limit("MSFT", clOrdId, side, price, qty);
    }

    /** Tests that must not be affected by fat-finger checks use symbols that never trade in this suite. */
    static Map<String, Object> limit(String symbol, String clOrdId, String side, String price, String qty) {
        return Map.of("clientOrderId", clOrdId, "symbol", symbol, "side", side, "type", "LIMIT",
                "price", price, "quantity", qty);
    }

    @Test
    void restOrderRestsThenFillsAgainstAnotherClient() {
        Response resting = post(TestEnvironment.KEY_1, "/api/v1/orders", limit(id(), "SELL", "410.50", "100"));
        assertThat(resting.status()).isEqualTo(201);
        assertThat(resting.body()).containsEntry("status", "NEW").containsEntry("accepted", true);

        Response taking = post(TestEnvironment.KEY_2, "/api/v1/orders", limit(id(), "BUY", "410.60", "40"));
        assertThat(taking.status()).isEqualTo(201);
        assertThat(taking.body()).containsEntry("status", "FILLED").containsEntry("filledQuantity", "40");
        List<Map<String, Object>> fills = (List<Map<String, Object>>) taking.body().get("fills");
        assertThat(fills).singleElement().satisfies(f -> {
            assertThat(f).containsEntry("price", "410.50").containsEntry("quantity", "40").containsEntry("liquidity", "TAKER");
        });
    }

    @Test
    void retryWithTheSameClientOrderIdReturnsTheOriginalOutcomeWithoutANewOrder() {
        String clOrdId = id();
        Response first = post(TestEnvironment.KEY_1, "/api/v1/orders", limit("NVDA", clOrdId, "BUY", "100.00", "10"));
        Response retry = post(TestEnvironment.KEY_1, "/api/v1/orders", limit("NVDA", clOrdId, "BUY", "100.00", "10"));

        assertThat(first.status()).isEqualTo(201);
        assertThat(retry.status()).isEqualTo(200);
        assertThat(retry.body()).containsEntry("replayed", true)
                .containsEntry("orderId", first.body().get("orderId"));
    }

    @Test
    void cancelAndReplaceThroughRest() {
        Response placed = post(TestEnvironment.KEY_1, "/api/v1/orders", limit("NVDA", id(), "BUY", "101.00", "10"));
        Object orderId = placed.body().get("orderId");

        Response replaced = post(TestEnvironment.KEY_1, "/api/v1/orders/" + orderId + "/replace",
                Map.of("clientOrderId", id(), "symbol", "NVDA", "price", "101.00", "quantity", "5"));
        assertThat(replaced.status()).isEqualTo(200);
        assertThat(replaced.body()).containsEntry("status", "REPLACED").containsEntry("leavesQuantity", "5");

        Response cancelled = post(TestEnvironment.KEY_1, "/api/v1/orders/" + orderId + "/cancel",
                Map.of("clientOrderId", id(), "symbol", "NVDA"));
        assertThat(cancelled.status()).isEqualTo(200);
        assertThat(cancelled.body()).containsEntry("status", "CANCELED");

        Response again = post(TestEnvironment.KEY_1, "/api/v1/orders/" + orderId + "/cancel",
                Map.of("clientOrderId", id(), "symbol", "NVDA"));
        assertThat(again.status()).isEqualTo(422);
        assertThat(again.body()).containsEntry("reason", "UNKNOWN_ORDER");
    }

    @Test
    void engineAndGatewayRejectsAre422WithAReason() {
        Response offTick = post(TestEnvironment.KEY_1, "/api/v1/orders", limit("TSLA", id(), "BUY", "1000.00", "1"));
        assertThat(offTick.status()).isEqualTo(422);
        assertThat(offTick.body()).containsEntry("reason", "PRICE_OUT_OF_BAND");

        Response tooBig = post(TestEnvironment.KEY_1, "/api/v1/orders", limit("TSLA", id(), "BUY", "300.00", "200000"));
        assertThat(tooBig.status()).isEqualTo(422);
        assertThat(tooBig.body()).containsEntry("reason", "RISK_MAX_ORDER_QUANTITY");
    }

    @Test
    void badInputIs400AndMissingKeyIs401() {
        assertThat(post(TestEnvironment.KEY_1, "/api/v1/orders", limit(id(), "BUY", "300.001", "1")).status())
                .isEqualTo(400);
        assertThat(post(TestEnvironment.KEY_1, "/api/v1/orders", limit("bad id!", "BUY", "300", "1")).status())
                .isEqualTo(400);
        assertThat(post(null, "/api/v1/orders", limit(id(), "BUY", "300", "1")).status()).isEqualTo(401);
        assertThat(post("wrong-key", "/api/v1/orders", limit(id(), "BUY", "300", "1")).status()).isEqualTo(401);
    }

    @Test
    void grpcOrderEntryAuthenticatesAndStreamsFillsOfRestingOrders() throws Exception {
        channel = ManagedChannelBuilder.forAddress("localhost", grpcServer.getPort()).usePlaintext().build();
        Metadata key1 = new Metadata();
        key1.put(Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER), TestEnvironment.KEY_1);
        var client1 = OrderEntryGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(key1));

        BlockingQueue<ClientExecutionReport> executions = new LinkedBlockingQueue<>();
        OrderEntryGrpc.newStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(key1))
                .streamExecutions(StreamExecutionsRequest.getDefaultInstance(), new StreamObserver<>() {
                    @Override
                    public void onNext(ClientExecutionReport value) {
                        executions.add(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
        Thread.sleep(200); // let the stream register

        String clOrdId = id();
        OrderResponse resting = client1.submitOrder(SubmitOrderRequest.newBuilder()
                .setClientOrderId(clOrdId).setSymbol("AAPL").setSide(Side.SIDE_SELL)
                .setOrderType(OrderType.ORDER_TYPE_LIMIT).setPrice("250.00").setQuantity("30").build());
        assertThat(resting.getAccepted()).isTrue();
        assertThat(resting.getStatus()).isEqualTo("NEW");

        Response taker = post(TestEnvironment.KEY_2, "/api/v1/orders",
                Map.of("clientOrderId", id(), "symbol", "AAPL", "side", "BUY", "type", "MARKET", "quantity", "30"));
        assertThat(taker.body()).containsEntry("status", "FILLED");

        ClientExecutionReport fill = null;
        for (int i = 0; i < 10 && fill == null; i++) {
            ClientExecutionReport r = executions.poll(5, TimeUnit.SECONDS);
            assertThat(r).isNotNull();
            if (r.getExecType() == ExecType.EXEC_TYPE_FILL) {
                fill = r;
            }
        }
        assertThat(fill.getClientOrderId()).as("internal ids are mapped back to the client's own").isEqualTo(clOrdId);
        assertThat(fill.getLastPrice()).isEqualTo("250.00");
        assertThat(fill.getLastQuantity()).isEqualTo("30");

        var anonymous = OrderEntryGrpc.newBlockingStub(channel);
        assertThatThrownBy(() -> anonymous.submitOrder(SubmitOrderRequest.newBuilder().setClientOrderId(id()).build()))
                .isInstanceOf(StatusRuntimeException.class)
                .extracting(e -> ((StatusRuntimeException) e).getStatus().getCode())
                .isEqualTo(Status.Code.UNAUTHENTICATED);
    }

    @Test
    void exposesPrometheusMetrics() {
        post(TestEnvironment.KEY_1, "/api/v1/orders", limit("NVDA", id(), "BUY", "100.00", "1"));
        String scrape = RestClient.create("http://localhost:" + httpPort).get().uri("/actuator/prometheus")
                .retrieve().body(String.class);
        assertThat(scrape).contains("matchbook_gateway_requests_total").contains("matchbook_gateway_request_seconds_bucket")
                .contains("resilience4j_circuitbreaker_state");
    }
}
