package dev.matchbook.gateway.order;

import dev.matchbook.gateway.admission.Admission;
import dev.matchbook.gateway.admission.RedisAdmission;
import dev.matchbook.gateway.config.GatewayProperties;
import dev.matchbook.gateway.engine.EngineClient;
import dev.matchbook.gateway.engine.EngineUnavailableException;
import dev.matchbook.gateway.reference.InvalidRequestException;
import dev.matchbook.gateway.reference.Symbol;
import dev.matchbook.gateway.reference.SymbolRegistry;
import dev.matchbook.gateway.risk.RiskManager;
import dev.matchbook.proto.v1.EngineCommand;
import dev.matchbook.proto.v1.EngineResult;
import dev.matchbook.proto.v1.ExecType;
import dev.matchbook.proto.v1.ExecutionReport;
import dev.matchbook.proto.v1.Liquidity;
import dev.matchbook.proto.v1.OrderType;
import dev.matchbook.proto.v1.Side;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.QueryTimeoutException;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * The order pipeline's decisions, with Redis and the engine mocked: what is stored for idempotent
 * replay, what is released for a safe retry, and what never reaches the engine.
 */
@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    private static final long CLIENT = 7;
    private static final Symbol AAPL = new Symbol("AAPL", 1, 0, 100, 1, 1, 1, 1_000_000, 10_000, 40_000);

    @Mock
    RedisAdmission admission;
    @Mock
    EngineClient engine;
    @Mock
    ExecutionRouter router;
    @Mock
    TracePropagation tracing;

    private RiskManager risk;
    private OrderService service;

    @BeforeEach
    void setUp() {
        GatewayProperties properties = new GatewayProperties("x", Duration.ofSeconds(1), "", Map.of(),
                new GatewayProperties.RateLimit(10, 10), Duration.ofHours(1),
                new GatewayProperties.Risk(1_000, new BigDecimal("1000000"), 5_000, new BigDecimal("10")),
                new GatewayProperties.Fix(false, 0, "MATCHBOOK"));
        risk = new RiskManager(properties);
        service = new OrderService(new SymbolRegistry(List.of(AAPL)), admission, risk, engine, router,
                JsonMapper.builder().build(), new GatewayMetrics(new SimpleMeterRegistry()), tracing);
    }

    static OrderRequest.Submit buy(String clOrdId, String price, String qty) {
        return new OrderRequest.Submit(clOrdId, "AAPL", Side.SIDE_BUY, OrderType.ORDER_TYPE_LIMIT, null, price, qty, null);
    }

    static EngineResult accepted(long orderId, long qty) {
        return EngineResult.newBuilder().addReports(ExecutionReport.newBuilder().setExecType(ExecType.EXEC_TYPE_NEW)
                .setOrderId(orderId).setLeavesQuantity(qty).setReason("NONE")).build();
    }

    @Test
    void acceptedOrderReachesTheEngineScaledAndWithTheInternalIdAndIsStored() {
        when(admission.admit(CLIENT, "o-1")).thenReturn(new Admission.Admitted(41));
        when(engine.submit(any())).thenReturn(accepted(900, 10));

        OrderOutcome outcome = service.handle(CLIENT, buy("o-1", "150.25", "10"));

        assertThat(outcome.result()).isEqualTo(OrderOutcome.Result.ACCEPTED);
        assertThat(outcome.orderId()).isEqualTo(900);
        ArgumentCaptor<EngineCommand> sent = ArgumentCaptor.forClass(EngineCommand.class);
        verify(engine).submit(sent.capture());
        assertThat(sent.getValue().getNewOrder().getPrice()).isEqualTo(15_025);
        assertThat(sent.getValue().getNewOrder().getClOrdId()).isEqualTo(41);
        assertThat(sent.getValue().getNewOrder().getClientId()).isEqualTo(CLIENT);
        verify(admission).complete(eq(CLIENT), eq("o-1"), contains("\"orderId\":900"));
        verify(router).remember(CLIENT, 41, "o-1");
        assertThat(risk.openBuy(CLIENT, 1)).as("reservation released after the answer").isZero();
    }

    @Test
    void engineTimeoutIsUnknownAndKeepsTheKeySoABlindRetryCannotDuplicate() {
        when(admission.admit(CLIENT, "o-2")).thenReturn(new Admission.Admitted(42));
        when(engine.submit(any())).thenThrow(new EngineUnavailableException("deadline", false, null));

        OrderOutcome outcome = service.handle(CLIENT, buy("o-2", "150.00", "10"));

        assertThat(outcome.result()).isEqualTo(OrderOutcome.Result.UNKNOWN);
        verify(admission).markUnknown(CLIENT, "o-2");
        verify(admission, never()).release(anyLong(), anyString());
        verify(admission, never()).complete(anyLong(), anyString(), anyString());
        assertThat(risk.openBuy(CLIENT, 1)).isZero();
    }

    @Test
    void requestThatNeverLeftTheGatewayReleasesTheKeySoARetryIsSafe() {
        when(admission.admit(CLIENT, "o-3")).thenReturn(new Admission.Admitted(43));
        when(engine.submit(any())).thenThrow(new EngineUnavailableException("circuit open", true, null));

        OrderOutcome outcome = service.handle(CLIENT, buy("o-3", "150.00", "10"));

        assertThat(outcome.result()).isEqualTo(OrderOutcome.Result.UNAVAILABLE);
        assertThat(outcome.reason()).isEqualTo("ENGINE_UNAVAILABLE");
        verify(admission).release(CLIENT, "o-3");
        verify(admission, never()).markUnknown(anyLong(), anyString());
    }

    @Test
    void riskRejectIsStoredAndNeverReachesTheEngine() {
        when(admission.admit(CLIENT, "o-4")).thenReturn(new Admission.Admitted(44));

        OrderOutcome outcome = service.handle(CLIENT, buy("o-4", "150.00", "5000"));

        assertThat(outcome.result()).isEqualTo(OrderOutcome.Result.REJECTED);
        assertThat(outcome.reason()).isEqualTo("RISK_MAX_ORDER_QUANTITY");
        verify(admission).complete(eq(CLIENT), eq("o-4"), contains("RISK_MAX_ORDER_QUANTITY"));
        verifyNoInteractions(engine);
    }

    @Test
    void completedDuplicateReplaysTheStoredOutcomeWithoutTheEngine() {
        when(admission.admit(CLIENT, "o-5")).thenReturn(new Admission.Completed("""
                {"result":"ACCEPTED","accepted":true,"orderId":77,"clientOrderId":"o-5","symbol":"AAPL","status":"NEW",
                 "reason":"","filledQuantity":"0","leavesQuantity":"10","fills":[],"replayed":false}"""));

        OrderOutcome outcome = service.handle(CLIENT, buy("o-5", "150.00", "10"));

        assertThat(outcome.replayed()).isTrue();
        assertThat(outcome.orderId()).isEqualTo(77);
        verifyNoInteractions(engine);
    }

    @Test
    void inFlightAndUnknownDuplicatesAreReportedDistinctly() {
        when(admission.admit(CLIENT, "o-6")).thenReturn(new Admission.InProgress(false));
        when(admission.admit(CLIENT, "o-7")).thenReturn(new Admission.InProgress(true));

        assertThat(service.handle(CLIENT, buy("o-6", "150.00", "1")).result()).isEqualTo(OrderOutcome.Result.IN_PROGRESS);
        assertThat(service.handle(CLIENT, buy("o-7", "150.00", "1")).result()).isEqualTo(OrderOutcome.Result.UNKNOWN);
        verifyNoInteractions(engine);
    }

    @Test
    void rateLimitedRequestsCarryTheRetryDelay() {
        when(admission.admit(CLIENT, "o-8")).thenReturn(new Admission.RateLimited(350));

        OrderOutcome outcome = service.handle(CLIENT, buy("o-8", "150.00", "1"));

        assertThat(outcome.result()).isEqualTo(OrderOutcome.Result.RATE_LIMITED);
        assertThat(outcome.reason()).isEqualTo("RATE_LIMITED:350");
        verifyNoInteractions(engine);
    }

    @Test
    void redisFailureFailsClosed() {
        when(admission.admit(CLIENT, "o-9")).thenThrow(new QueryTimeoutException("redis down"));

        OrderOutcome outcome = service.handle(CLIENT, buy("o-9", "150.00", "1"));

        assertThat(outcome.result()).isEqualTo(OrderOutcome.Result.UNAVAILABLE);
        assertThat(outcome.reason()).isEqualTo("REDIS_UNAVAILABLE");
        verifyNoInteractions(engine);
    }

    @Test
    void invalidInputIsRejectedBeforeRedisIsTouched() {
        assertThatThrownBy(() -> service.handle(CLIENT, buy("o-10", "150.001", "1")))
                .isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> service.handle(CLIENT, buy("bad id", "150.00", "1")))
                .isInstanceOf(InvalidRequestException.class);
        verifyNoInteractions(admission, engine);
    }

    @Test
    void fillsAreSummarisedForTheOrderThatWasSent() {
        when(admission.admit(CLIENT, "o-11")).thenReturn(new Admission.Admitted(45));
        when(engine.submit(any())).thenReturn(EngineResult.newBuilder()
                .addReports(ExecutionReport.newBuilder().setExecType(ExecType.EXEC_TYPE_NEW).setOrderId(901).setReason("NONE"))
                .addReports(ExecutionReport.newBuilder().setExecType(ExecType.EXEC_TYPE_FILL).setOrderId(901)
                        .setLastPrice(15_000).setLastQuantity(10).setCumQuantity(10).setTradeId(5)
                        .setLiquidity(Liquidity.LIQUIDITY_TAKER).setReason("NONE"))
                .build());

        OrderOutcome outcome = service.handle(CLIENT, buy("o-11", "150.00", "10"));

        assertThat(outcome.status()).isEqualTo("FILLED");
        assertThat(outcome.filledQuantity()).isEqualTo("10");
        assertThat(outcome.fills()).singleElement().satisfies(f -> {
            assertThat(f.price()).isEqualTo("150.00");
            assertThat(f.liquidity()).isEqualTo("TAKER");
        });
    }
}
