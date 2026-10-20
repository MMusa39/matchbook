package dev.matchbook.gateway.order;

import dev.matchbook.gateway.admission.Admission;
import dev.matchbook.gateway.admission.RedisAdmission;
import dev.matchbook.gateway.engine.EngineClient;
import dev.matchbook.gateway.engine.EngineUnavailableException;
import dev.matchbook.gateway.reference.InvalidRequestException;
import dev.matchbook.gateway.reference.Symbol;
import dev.matchbook.gateway.reference.SymbolRegistry;
import dev.matchbook.gateway.risk.RiskCheck;
import dev.matchbook.gateway.risk.RiskManager;
import dev.matchbook.proto.v1.Cancel;
import dev.matchbook.proto.v1.EngineCommand;
import dev.matchbook.proto.v1.EngineResult;
import dev.matchbook.proto.v1.ExecType;
import dev.matchbook.proto.v1.ExecutionReport;
import dev.matchbook.proto.v1.NewOrder;
import dev.matchbook.proto.v1.OrderType;
import dev.matchbook.proto.v1.Replace;
import dev.matchbook.proto.v1.Side;
import dev.matchbook.proto.v1.StpMode;
import dev.matchbook.proto.v1.TimeInForce;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The order-entry pipeline inside the gateway, shared by REST and gRPC:
 * <ol>
 *     <li>validate and convert decimals to fixed point;</li>
 *     <li>admit through Redis: idempotency, rate limit, internal id (one Lua call);</li>
 *     <li>pre-trade risk (new orders);</li>
 *     <li>call the engine (deadline + circuit breaker);</li>
 *     <li>store the outcome under the client order id, so a retry returns the same answer.</li>
 * </ol>
 * Gateway rejects (risk) are stored too: a retried order that failed risk fails the same way.
 */
@Service
public class OrderService {

    private static final Logger LOG = LoggerFactory.getLogger(OrderService.class);
    private static final Pattern CLIENT_ORDER_ID = Pattern.compile("[A-Za-z0-9._:-]{1,64}");

    private final SymbolRegistry symbols;
    private final RedisAdmission admission;
    private final RiskManager risk;
    private final EngineClient engine;
    private final ExecutionRouter router;
    private final JsonMapper json;
    private final GatewayMetrics metrics;
    private final TracePropagation tracing;

    public OrderService(SymbolRegistry symbols, RedisAdmission admission, RiskManager risk, EngineClient engine,
                        ExecutionRouter router, JsonMapper json, GatewayMetrics metrics, TracePropagation tracing) {
        this.symbols = symbols;
        this.admission = admission;
        this.risk = risk;
        this.engine = engine;
        this.router = router;
        this.json = json;
        this.metrics = metrics;
        this.tracing = tracing;
    }

    public OrderOutcome handle(long clientId, OrderRequest request) {
        Timer.Sample sample = metrics.start();
        String kind = switch (request) {
            case OrderRequest.Submit s -> "submit";
            case OrderRequest.Cancel c -> "cancel";
            case OrderRequest.Replace r -> "replace";
        };
        OrderOutcome outcome = process(clientId, request);
        metrics.record(sample, kind, outcome);
        return outcome;
    }

    private OrderOutcome process(long clientId, OrderRequest request) {
        if (request.clientOrderId() == null || !CLIENT_ORDER_ID.matcher(request.clientOrderId()).matches()) {
            throw new InvalidRequestException("clientOrderId must be 1-64 characters of [A-Za-z0-9._:-]");
        }
        Symbol symbol = symbols.bySymbol(request.symbol());
        Prepared prepared = prepare(request, symbol);

        Admission admitted;
        try {
            admitted = admission.admit(clientId, request.clientOrderId());
        } catch (DataAccessException e) {
            LOG.warn("Redis unavailable, rejecting order: {}", e.toString());
            return OrderOutcome.failure(OrderOutcome.Result.UNAVAILABLE, request.clientOrderId(), symbol.symbol(),
                    "REDIS_UNAVAILABLE");
        }
        long internalId;
        switch (admitted) {
            case Admission.RateLimited limited -> {
                return OrderOutcome.failure(OrderOutcome.Result.RATE_LIMITED, request.clientOrderId(),
                        symbol.symbol(), "RATE_LIMITED:" + limited.retryAfterMillis());
            }
            case Admission.Completed done -> {
                return json.readValue(done.storedOutcome(), OrderOutcome.class).asReplay();
            }
            case Admission.InProgress inProgress -> {
                return OrderOutcome.failure(inProgress.outcomeUnknown() ? OrderOutcome.Result.UNKNOWN
                                : OrderOutcome.Result.IN_PROGRESS, request.clientOrderId(), symbol.symbol(),
                        inProgress.outcomeUnknown() ? "OUTCOME_UNKNOWN" : "DUPLICATE_IN_PROGRESS");
            }
            case Admission.Admitted a -> internalId = a.internalClOrdId();
        }
        router.remember(clientId, internalId, request.clientOrderId());

        RiskManager.Reservation reservation = null;
        if (request instanceof OrderRequest.Submit submit) {
            RiskCheck check = risk.checkNewOrder(clientId, symbol, submit.side(), submit.orderType(),
                    prepared.price(), prepared.quantity());
            if (!check.passed()) {
                OrderOutcome rejected = OrderOutcome.failure(OrderOutcome.Result.REJECTED, request.clientOrderId(),
                        symbol.symbol(), check.rejectReason());
                admission.complete(clientId, request.clientOrderId(), json.writeValueAsString(rejected));
                return rejected;
            }
            reservation = check.reservation();
        }

        EngineResult result;
        try {
            result = engine.submit(command(clientId, internalId, request, symbol, prepared));
        } catch (EngineUnavailableException e) {
            if (e.definitelyNotSent()) {
                admission.release(clientId, request.clientOrderId());
                return OrderOutcome.failure(OrderOutcome.Result.UNAVAILABLE, request.clientOrderId(),
                        symbol.symbol(), "ENGINE_UNAVAILABLE");
            }
            admission.markUnknown(clientId, request.clientOrderId());
            return OrderOutcome.failure(OrderOutcome.Result.UNKNOWN, request.clientOrderId(), symbol.symbol(),
                    "OUTCOME_UNKNOWN");
        } finally {
            risk.release(reservation);
        }
        OrderOutcome outcome = toOutcome(result, request.clientOrderId(), symbol);
        admission.complete(clientId, request.clientOrderId(), json.writeValueAsString(outcome));
        return outcome;
    }

    private record Prepared(long price, long quantity) {
    }

    private static Prepared prepare(OrderRequest request, Symbol symbol) {
        return switch (request) {
            case OrderRequest.Submit s -> {
                if (s.side() == null || s.side() == Side.SIDE_UNSPECIFIED || s.side() == Side.UNRECOGNIZED) {
                    throw new InvalidRequestException("side is required");
                }
                if (s.orderType() == null || s.orderType() == OrderType.ORDER_TYPE_UNSPECIFIED
                        || s.orderType() == OrderType.UNRECOGNIZED) {
                    throw new InvalidRequestException("orderType is required");
                }
                long price = s.orderType() == OrderType.ORDER_TYPE_MARKET ? 0 : symbol.parsePrice(s.price());
                yield new Prepared(price, symbol.parseQuantity(s.quantity()));
            }
            case OrderRequest.Cancel c -> new Prepared(0, 0);
            case OrderRequest.Replace r -> new Prepared(symbol.parsePrice(r.price()), symbol.parseQuantity(r.quantity()));
        };
    }

    private EngineCommand command(long clientId, long internalId, OrderRequest request, Symbol symbol,
                                  Prepared prepared) {
        EngineCommand.Builder builder = EngineCommand.newBuilder();
        switch (request) {
            case OrderRequest.Submit s -> builder.setNewOrder(NewOrder.newBuilder()
                    .setClientId(clientId).setClOrdId(internalId).setSymbolId(symbol.id())
                    .setSide(s.side()).setOrderType(s.orderType())
                    .setTimeInForce(s.timeInForce() == null || s.timeInForce() == TimeInForce.TIME_IN_FORCE_UNSPECIFIED
                            ? defaultTimeInForce(s.orderType()) : s.timeInForce())
                    .setStpMode(s.stpMode() == null ? StpMode.STP_MODE_CANCEL_RESTING : s.stpMode())
                    .setPrice(prepared.price()).setQuantity(prepared.quantity()));
            case OrderRequest.Cancel c -> builder.setCancel(Cancel.newBuilder()
                    .setClientId(clientId).setClOrdId(internalId).setSymbolId(symbol.id()).setOrderId(c.orderId()));
            case OrderRequest.Replace r -> builder.setReplace(Replace.newBuilder()
                    .setClientId(clientId).setClOrdId(internalId).setSymbolId(symbol.id()).setOrderId(r.orderId())
                    .setPrice(prepared.price()).setQuantity(prepared.quantity()));
        }
        String traceparent = tracing.currentTraceparent();
        if (traceparent != null) {
            builder.setTraceparent(traceparent);
        }
        return builder.build();
    }

    private static TimeInForce defaultTimeInForce(OrderType type) {
        return type == OrderType.ORDER_TYPE_MARKET ? TimeInForce.TIME_IN_FORCE_IOC : TimeInForce.TIME_IN_FORCE_GTC;
    }

    static OrderOutcome toOutcome(EngineResult result, String clientOrderId, Symbol symbol) {
        List<ExecutionReport> reports = result.getReportsList();
        if (reports.isEmpty()) {
            return OrderOutcome.failure(OrderOutcome.Result.REJECTED, clientOrderId, symbol.symbol(), "NO_RESPONSE");
        }
        ExecutionReport first = reports.getFirst();
        boolean accepted = switch (first.getExecType()) {
            case EXEC_TYPE_NEW, EXEC_TYPE_CANCELED, EXEC_TYPE_REPLACED -> true;
            default -> false;
        };
        // The last report about the request's own order decides its status.
        ExecutionReport last = first;
        List<OrderOutcome.FillView> fills = new ArrayList<>();
        for (ExecutionReport r : reports) {
            if (r.getOrderId() != first.getOrderId()) {
                continue;
            }
            last = r;
            if (r.getExecType() == ExecType.EXEC_TYPE_PARTIAL_FILL || r.getExecType() == ExecType.EXEC_TYPE_FILL) {
                fills.add(new OrderOutcome.FillView(r.getTradeId(), symbol.formatPrice(r.getLastPrice()),
                        Long.toString(r.getLastQuantity()), r.getLiquidity().name().replace("LIQUIDITY_", "")));
            }
        }
        String status = switch (last.getExecType()) {
            case EXEC_TYPE_NEW -> "NEW";
            case EXEC_TYPE_PARTIAL_FILL -> "PARTIALLY_FILLED";
            case EXEC_TYPE_FILL -> "FILLED";
            case EXEC_TYPE_CANCELED -> "CANCELED";
            case EXEC_TYPE_REPLACED -> "REPLACED";
            default -> "REJECTED";
        };
        String reason = accepted && last.getExecType() != ExecType.EXEC_TYPE_CANCELED ? "" : last.getReason();
        return new OrderOutcome(accepted ? OrderOutcome.Result.ACCEPTED : OrderOutcome.Result.REJECTED, accepted,
                first.getOrderId(), clientOrderId, symbol.symbol(), status, reason == null ? "" : reason,
                Long.toString(last.getCumQuantity()), Long.toString(last.getLeavesQuantity()), fills, false);
    }
}
