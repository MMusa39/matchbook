package dev.matchbook.gateway.risk;

import dev.matchbook.gateway.config.GatewayProperties;
import dev.matchbook.gateway.reference.Symbol;
import dev.matchbook.proto.v1.ExecType;
import dev.matchbook.proto.v1.ExecutionReport;
import dev.matchbook.proto.v1.OrderType;
import dev.matchbook.proto.v1.Side;
import dev.matchbook.proto.v1.Trade;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RiskManagerTest {

    private static final Symbol AAPL = new Symbol("AAPL", 1, 0, 100, 1, 1, 1, 1_000_000, 10_000, 40_000);
    private static final OrderType LIMIT = OrderType.ORDER_TYPE_LIMIT;

    private final RiskManager risk = new RiskManager(new GatewayProperties("x", Duration.ofSeconds(1), "",
            Map.of(), new GatewayProperties.RateLimit(10, 1), Duration.ofHours(1),
            new GatewayProperties.Risk(1_000, new BigDecimal("100000"), 1_500, new BigDecimal("5"))));

    @Test
    void rejectsOversizedOrders() {
        assertThat(risk.checkNewOrder(1, AAPL, Side.SIDE_BUY, LIMIT, 15_000, 1_001).rejectReason())
                .isEqualTo("RISK_MAX_ORDER_QUANTITY");
    }

    @Test
    void rejectsExcessiveNotional() {
        // 1000 x 150.00 = 150,000 > 100,000
        assertThat(risk.checkNewOrder(1, AAPL, Side.SIDE_BUY, LIMIT, 15_000, 1_000).rejectReason())
                .isEqualTo("RISK_MAX_NOTIONAL");
        assertThat(risk.checkNewOrder(1, AAPL, Side.SIDE_BUY, LIMIT, 15_000, 600).passed()).isTrue();
    }

    @Test
    void valuesMarketBuysAtTheTopOfTheBand() {
        // 300 x 400.00 = 120,000 > 100,000 even though a limit at 150 would pass
        assertThat(risk.checkNewOrder(1, AAPL, Side.SIDE_BUY, OrderType.ORDER_TYPE_MARKET, 0, 300).rejectReason())
                .isEqualTo("RISK_MAX_NOTIONAL");
    }

    @Test
    void rejectsFatFingerPricesOnceThereIsALastTrade() {
        assertThat(risk.checkNewOrder(1, AAPL, Side.SIDE_BUY, LIMIT, 20_000, 1).passed()).isTrue();
        risk.onTrade(Trade.newBuilder().setSymbolId(1).setPrice(15_000).build());
        assertThat(risk.checkNewOrder(1, AAPL, Side.SIDE_BUY, LIMIT, 15_800, 1).rejectReason()).isEqualTo("RISK_FAT_FINGER");
        assertThat(risk.checkNewOrder(1, AAPL, Side.SIDE_BUY, LIMIT, 15_700, 1).passed()).isTrue();
    }

    @Test
    void positionLimitCountsFillsOpenOrdersAndReservations() {
        risk.onExecutionReport(report(10, ExecType.EXEC_TYPE_FILL, Side.SIDE_BUY, 500, 0));
        risk.onExecutionReport(report(11, ExecType.EXEC_TYPE_NEW, Side.SIDE_BUY, 0, 400));
        assertThat(risk.position(1, 1)).isEqualTo(500);

        RiskCheck reserved = risk.checkNewOrder(1, AAPL, Side.SIDE_BUY, LIMIT, 1_000, 500);
        assertThat(reserved.passed()).isTrue();
        // 500 filled + 400 open + 500 reserved = 1400; another 101 would breach 1500.
        assertThat(risk.checkNewOrder(1, AAPL, Side.SIDE_BUY, LIMIT, 1_000, 101).rejectReason())
                .isEqualTo("RISK_POSITION_LIMIT");
        risk.release(reserved.reservation());
        assertThat(risk.checkNewOrder(1, AAPL, Side.SIDE_BUY, LIMIT, 1_000, 101).passed()).isTrue();
    }

    @Test
    void cancelsAndFillsReleaseOpenExposure() {
        risk.onExecutionReport(report(20, ExecType.EXEC_TYPE_NEW, Side.SIDE_SELL, 0, 300));
        assertThat(risk.openSell(1, 1)).isEqualTo(300);
        risk.onExecutionReport(report(20, ExecType.EXEC_TYPE_PARTIAL_FILL, Side.SIDE_SELL, 100, 200));
        assertThat(risk.openSell(1, 1)).isEqualTo(200);
        assertThat(risk.position(1, 1)).isEqualTo(-100);
        risk.onExecutionReport(report(20, ExecType.EXEC_TYPE_CANCELED, Side.SIDE_SELL, 0, 0));
        assertThat(risk.openSell(1, 1)).isZero();
    }

    private static ExecutionReport report(long orderId, ExecType type, Side side, long lastQty, long leaves) {
        return ExecutionReport.newBuilder().setClientId(1).setSymbolId(1).setOrderId(orderId).setExecType(type)
                .setSide(side).setLastQuantity(lastQty).setLeavesQuantity(leaves).build();
    }
}
