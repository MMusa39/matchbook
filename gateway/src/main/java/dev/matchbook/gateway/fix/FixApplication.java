package dev.matchbook.gateway.fix;

import dev.matchbook.gateway.api.ApiKeyAuthenticator;
import dev.matchbook.gateway.order.ExecutionRouter;
import dev.matchbook.gateway.order.OrderOutcome;
import dev.matchbook.gateway.order.OrderRequest;
import dev.matchbook.gateway.order.OrderService;
import dev.matchbook.gateway.reference.InvalidRequestException;
import dev.matchbook.proto.v1.ClientExecutionReport;
import dev.matchbook.proto.v1.ExecType;
import dev.matchbook.proto.v1.OrderType;
import dev.matchbook.proto.v1.Side;
import dev.matchbook.proto.v1.StpMode;
import dev.matchbook.proto.v1.TimeInForce;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import quickfix.Application;
import quickfix.FieldNotFound;
import quickfix.Message;
import quickfix.RejectLogon;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.field.AvgPx;
import quickfix.field.ClOrdID;
import quickfix.field.CumQty;
import quickfix.field.CxlRejReason;
import quickfix.field.CxlRejResponseTo;
import quickfix.field.ExecID;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.LeavesQty;
import quickfix.field.MsgType;
import quickfix.field.OrdRejReason;
import quickfix.field.OrdStatus;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.OrigClOrdID;
import quickfix.field.Password;
import quickfix.field.Price;
import quickfix.field.Text;
import quickfix.field.TransactTime;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.NewOrderSingle;
import quickfix.fix44.OrderCancelReject;
import quickfix.fix44.OrderCancelReplaceRequest;
import quickfix.fix44.OrderCancelRequest;

import java.math.BigDecimal;
import java.math.MathContext;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FIX 4.4 session logic.
 *
 * <ul>
 *     <li><b>Logon:</b> {@code Password (554)} must be a valid API key; the session is then bound to that
 *     client id.</li>
 *     <li><b>Orders:</b> {@code NewOrderSingle (D)}, {@code OrderCancelRequest (F)} and
 *     {@code OrderCancelReplaceRequest (G)} go through the shared {@link OrderService}. Prices are read as
 *     the field's raw text, not as a {@code double}.</li>
 *     <li><b>Reports:</b> execution reports come from the engine's event stream via
 *     {@link ExecutionRouter}, so fills of resting orders arrive asynchronously, as on a real venue.
 *     Requests rejected by the gateway itself (risk, rate limit, duplicate ClOrdID) never reach the engine
 *     and are answered directly. Engine cancel/replace rejects become {@code OrderCancelReject (9)}.</li>
 *     <li><b>Cancel/replace</b> need {@code OrderID (37)}, the engine order id from the order's reports.</li>
 * </ul>
 */
@Component
public class FixApplication implements Application {

    private static final Logger LOG = LoggerFactory.getLogger(FixApplication.class);
    private static final MathContext MC = MathContext.DECIMAL64;

    private final OrderService orders;
    private final ExecutionRouter router;
    private final ApiKeyAuthenticator authenticator;
    private final Map<SessionID, Long> clients = new ConcurrentHashMap<>();
    private final Map<SessionID, Runnable> subscriptions = new ConcurrentHashMap<>();
    /** Per order: cumulative quantity and notional, for AvgPx (6), which FIX 4.4 requires. */
    private final Map<Long, BigDecimal[]> fills = new ConcurrentHashMap<>();

    public FixApplication(OrderService orders, ExecutionRouter router, ApiKeyAuthenticator authenticator) {
        this.orders = orders;
        this.router = router;
        this.authenticator = authenticator;
    }

    // ------------------------------------------------------------------ session lifecycle

    @Override
    public void onCreate(SessionID sessionId) {
    }

    @Override
    public void fromAdmin(Message message, SessionID sessionId) throws FieldNotFound, RejectLogon {
        if (MsgType.LOGON.equals(message.getHeader().getString(MsgType.FIELD))) {
            String key = message.isSetField(Password.FIELD) ? message.getString(Password.FIELD) : null;
            OptionalLong client = authenticator.authenticate(key);
            if (client.isEmpty()) {
                LOG.info("FIX logon rejected for {}: missing or invalid Password (554)", sessionId);
                throw new RejectLogon("invalid credentials");
            }
            clients.put(sessionId, client.getAsLong());
        }
    }

    @Override
    public void onLogon(SessionID sessionId) {
        Long client = clients.get(sessionId);
        if (client != null) {
            subscriptions.put(sessionId, router.subscribe(client, report -> send(sessionId, toFix(report))));
            LOG.info("FIX session {} logged on as client {}", sessionId, client);
        }
    }

    @Override
    public void onLogout(SessionID sessionId) {
        Runnable unsubscribe = subscriptions.remove(sessionId);
        if (unsubscribe != null) {
            unsubscribe.run();
        }
        clients.remove(sessionId);
    }

    @Override
    public void toAdmin(Message message, SessionID sessionId) {
    }

    @Override
    public void toApp(Message message, SessionID sessionId) {
    }

    // ------------------------------------------------------------------ orders

    @Override
    public void fromApp(Message message, SessionID sessionId) throws FieldNotFound {
        Long client = clients.get(sessionId);
        if (client == null) {
            return;
        }
        switch (message) {
            case NewOrderSingle m -> onNewOrder(m, client, sessionId);
            case OrderCancelRequest m -> onCancel(m, client, sessionId);
            case OrderCancelReplaceRequest m -> onReplace(m, client, sessionId);
            default -> LOG.debug("ignoring {} from {}", message.getHeader().getString(MsgType.FIELD), sessionId);
        }
    }

    private void onNewOrder(NewOrderSingle m, long client, SessionID session) throws FieldNotFound {
        String clOrdId = m.getString(ClOrdID.FIELD);
        String symbol = m.getSymbol().getValue();
        char side = m.getSide().getValue();
        char ordType = m.getOrdType().getValue();
        try {
            OrderRequest.Submit request = new OrderRequest.Submit(clOrdId, symbol,
                    side == quickfix.field.Side.BUY ? Side.SIDE_BUY : side == quickfix.field.Side.SELL ? Side.SIDE_SELL
                            : Side.SIDE_UNSPECIFIED,
                    ordType == quickfix.field.OrdType.MARKET ? OrderType.ORDER_TYPE_MARKET
                            : ordType == quickfix.field.OrdType.LIMIT ? OrderType.ORDER_TYPE_LIMIT
                            : OrderType.ORDER_TYPE_UNSPECIFIED,
                    timeInForce(m),
                    m.isSetField(Price.FIELD) ? m.getString(Price.FIELD) : null,
                    m.getString(OrderQty.FIELD),
                    StpMode.STP_MODE_CANCEL_RESTING);
            OrderOutcome outcome = orders.handle(client, request);
            if (rejectedByGateway(outcome)) {
                send(session, reject(clOrdId, symbol, side, m.getString(OrderQty.FIELD),
                        outcome.replayed() ? "DUPLICATE_CLORDID" : outcome.reason()));
            }
        } catch (InvalidRequestException e) {
            send(session, reject(clOrdId, symbol, side, m.getString(OrderQty.FIELD), e.getMessage()));
        }
    }

    private void onCancel(OrderCancelRequest m, long client, SessionID session) throws FieldNotFound {
        String clOrdId = m.getString(ClOrdID.FIELD);
        String orig = m.isSetField(OrigClOrdID.FIELD) ? m.getString(OrigClOrdID.FIELD) : "";
        if (!m.isSetField(OrderID.FIELD)) {
            send(session, cancelReject(clOrdId, orig, "NONE", CxlRejResponseTo.ORDER_CANCEL_REQUEST,
                    "OrderID (37) is required"));
            return;
        }
        try {
            OrderOutcome outcome = orders.handle(client, new OrderRequest.Cancel(clOrdId, m.getSymbol().getValue(),
                    Long.parseLong(m.getString(OrderID.FIELD))));
            if (rejectedByGateway(outcome)) {
                send(session, cancelReject(clOrdId, orig, m.getString(OrderID.FIELD),
                        CxlRejResponseTo.ORDER_CANCEL_REQUEST, outcome.replayed() ? "DUPLICATE_CLORDID" : outcome.reason()));
            }
        } catch (InvalidRequestException | NumberFormatException e) {
            send(session, cancelReject(clOrdId, orig, m.getString(OrderID.FIELD), CxlRejResponseTo.ORDER_CANCEL_REQUEST,
                    e.getMessage()));
        }
    }

    private void onReplace(OrderCancelReplaceRequest m, long client, SessionID session) throws FieldNotFound {
        String clOrdId = m.getString(ClOrdID.FIELD);
        String orig = m.isSetField(OrigClOrdID.FIELD) ? m.getString(OrigClOrdID.FIELD) : "";
        if (!m.isSetField(OrderID.FIELD)) {
            send(session, cancelReject(clOrdId, orig, "NONE", CxlRejResponseTo.ORDER_CANCEL_REPLACE_REQUEST,
                    "OrderID (37) is required"));
            return;
        }
        try {
            OrderOutcome outcome = orders.handle(client, new OrderRequest.Replace(clOrdId, m.getSymbol().getValue(),
                    Long.parseLong(m.getString(OrderID.FIELD)),
                    m.isSetField(Price.FIELD) ? m.getString(Price.FIELD) : null, m.getString(OrderQty.FIELD)));
            if (rejectedByGateway(outcome)) {
                send(session, cancelReject(clOrdId, orig, m.getString(OrderID.FIELD),
                        CxlRejResponseTo.ORDER_CANCEL_REPLACE_REQUEST,
                        outcome.replayed() ? "DUPLICATE_CLORDID" : outcome.reason()));
            }
        } catch (InvalidRequestException | NumberFormatException e) {
            send(session, cancelReject(clOrdId, orig, m.getString(OrderID.FIELD),
                    CxlRejResponseTo.ORDER_CANCEL_REPLACE_REQUEST, e.getMessage()));
        }
    }

    private static TimeInForce timeInForce(NewOrderSingle m) throws FieldNotFound {
        if (!m.isSetField(quickfix.field.TimeInForce.FIELD)) {
            return null; // gateway default: GTC for limit, IOC for market
        }
        return switch (m.getTimeInForce().getValue()) {
            case quickfix.field.TimeInForce.IMMEDIATE_OR_CANCEL -> TimeInForce.TIME_IN_FORCE_IOC;
            case quickfix.field.TimeInForce.FILL_OR_KILL -> TimeInForce.TIME_IN_FORCE_FOK;
            default -> TimeInForce.TIME_IN_FORCE_GTC; // DAY and GTC both rest for the session
        };
    }

    /**
     * True when the request ended in the gateway (risk, rate limit, duplicate, infrastructure) and so will
     * produce no engine report. Engine rejects arrive on the event stream like every other report.
     */
    static boolean rejectedByGateway(OrderOutcome outcome) {
        if (outcome.replayed()) {
            return true; // a duplicate ClOrdID: FIX venues reject it rather than re-sending the original
        }
        if (outcome.result() == OrderOutcome.Result.ACCEPTED) {
            return false;
        }
        String r = outcome.reason() == null ? "" : outcome.reason();
        return outcome.result() != OrderOutcome.Result.REJECTED || r.startsWith("RISK_");
    }

    // ------------------------------------------------------------------ outbound messages

    Message toFix(ClientExecutionReport r) {
        if (r.getExecType() == ExecType.EXEC_TYPE_CANCEL_REJECTED || r.getExecType() == ExecType.EXEC_TYPE_REPLACE_REJECTED) {
            return cancelReject(r.getClientOrderId(), r.getOrigClientOrderId(), Long.toString(r.getOrderId()),
                    r.getExecType() == ExecType.EXEC_TYPE_CANCEL_REJECTED ? CxlRejResponseTo.ORDER_CANCEL_REQUEST
                            : CxlRejResponseTo.ORDER_CANCEL_REPLACE_REQUEST, r.getReason());
        }
        char execType = switch (r.getExecType()) {
            case EXEC_TYPE_NEW -> quickfix.field.ExecType.NEW;
            case EXEC_TYPE_PARTIAL_FILL, EXEC_TYPE_FILL -> quickfix.field.ExecType.TRADE;
            case EXEC_TYPE_CANCELED -> quickfix.field.ExecType.CANCELED;
            case EXEC_TYPE_REPLACED -> quickfix.field.ExecType.REPLACED;
            default -> quickfix.field.ExecType.REJECTED;
        };
        BigDecimal cum = new BigDecimal(r.getCumQuantity());
        BigDecimal leaves = new BigDecimal(r.getLeavesQuantity());
        char ordStatus = switch (r.getExecType()) {
            case EXEC_TYPE_NEW -> OrdStatus.NEW;
            case EXEC_TYPE_PARTIAL_FILL -> OrdStatus.PARTIALLY_FILLED;
            case EXEC_TYPE_FILL -> OrdStatus.FILLED;
            case EXEC_TYPE_CANCELED -> OrdStatus.CANCELED;
            case EXEC_TYPE_REPLACED -> cum.signum() > 0 ? OrdStatus.PARTIALLY_FILLED : OrdStatus.NEW;
            default -> OrdStatus.REJECTED;
        };
        ExecutionReport m = new ExecutionReport(
                new OrderID(Long.toString(r.getOrderId())),
                new ExecID(r.getOrderId() + "-" + r.getSeq()),
                new quickfix.field.ExecType(execType),
                new OrdStatus(ordStatus),
                new quickfix.field.Side(r.getSide() == Side.SIDE_BUY ? quickfix.field.Side.BUY : quickfix.field.Side.SELL),
                new LeavesQty(leaves.doubleValue()),
                new CumQty(cum.doubleValue()),
                new AvgPx(0));
        m.setString(ClOrdID.FIELD, r.getClientOrderId());
        if (!r.getOrigClientOrderId().isEmpty()) {
            m.setString(OrigClOrdID.FIELD, r.getOrigClientOrderId());
        }
        m.setString(quickfix.field.Symbol.FIELD, r.getSymbol());
        m.setString(OrderQty.FIELD, r.getQuantity());
        if (r.getOrderType() == OrderType.ORDER_TYPE_LIMIT) {
            m.setString(Price.FIELD, r.getPrice());
        }
        m.setString(AvgPx.FIELD, averagePrice(r).toPlainString());
        if (execType == quickfix.field.ExecType.TRADE) {
            m.setString(LastPx.FIELD, r.getLastPrice());
            m.setString(LastQty.FIELD, r.getLastQuantity());
            // FIX 4.4 has no TradeID (1003, added in 5.0); the venue's trade id goes in SecondaryExecID.
            m.setString(quickfix.field.SecondaryExecID.FIELD, Long.toString(r.getTradeId()));
        }
        if (execType == quickfix.field.ExecType.REJECTED) {
            m.setInt(OrdRejReason.FIELD, OrdRejReason.OTHER);
        }
        if (!r.getReason().isEmpty() && !"NONE".equals(r.getReason())) {
            m.setString(Text.FIELD, r.getReason());
        }
        m.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));
        return m;
    }

    /** Volume-weighted average fill price of the order so far, tracked from its fills. */
    private BigDecimal averagePrice(ClientExecutionReport r) {
        boolean fill = r.getExecType() == ExecType.EXEC_TYPE_PARTIAL_FILL || r.getExecType() == ExecType.EXEC_TYPE_FILL;
        BigDecimal[] state = fill
                ? fills.computeIfAbsent(r.getOrderId(), id -> new BigDecimal[]{BigDecimal.ZERO, BigDecimal.ZERO})
                : fills.get(r.getOrderId());
        BigDecimal avg = BigDecimal.ZERO;
        if (state != null) {
            synchronized (state) {
                if (fill) {
                    BigDecimal qty = new BigDecimal(r.getLastQuantity());
                    state[0] = state[0].add(qty);
                    state[1] = state[1].add(qty.multiply(new BigDecimal(r.getLastPrice()), MC));
                }
                if (state[0].signum() > 0) {
                    avg = state[1].divide(state[0], MC).stripTrailingZeros();
                }
            }
        }
        boolean done = r.getExecType() == ExecType.EXEC_TYPE_FILL || r.getExecType() == ExecType.EXEC_TYPE_CANCELED
                || r.getExecType() == ExecType.EXEC_TYPE_REJECTED;
        if (done) {
            fills.remove(r.getOrderId());
        }
        return avg;
    }

    private static ExecutionReport reject(String clOrdId, String symbol, char side, String quantity, String reason) {
        ExecutionReport m = new ExecutionReport(new OrderID("NONE"), new ExecID("REJ-" + clOrdId),
                new quickfix.field.ExecType(quickfix.field.ExecType.REJECTED), new OrdStatus(OrdStatus.REJECTED),
                new quickfix.field.Side(side), new LeavesQty(0), new CumQty(0), new AvgPx(0));
        m.setString(ClOrdID.FIELD, clOrdId);
        m.setString(quickfix.field.Symbol.FIELD, symbol);
        m.setString(OrderQty.FIELD, quantity);
        m.setInt(OrdRejReason.FIELD, reason != null && reason.startsWith("DUPLICATE") ? OrdRejReason.DUPLICATE_ORDER
                : OrdRejReason.OTHER);
        m.setString(Text.FIELD, reason == null || reason.isBlank() ? "REJECTED" : reason);
        m.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));
        return m;
    }

    private static OrderCancelReject cancelReject(String clOrdId, String orig, String orderId, char responseTo,
                                                  String reason) {
        OrderCancelReject m = new OrderCancelReject(new OrderID(orderId), new ClOrdID(clOrdId),
                new OrigClOrdID(orig == null || orig.isEmpty() ? "NONE" : orig), new OrdStatus(OrdStatus.REJECTED),
                new CxlRejResponseTo(responseTo));
        m.setInt(CxlRejReason.FIELD, "UNKNOWN_ORDER".equals(reason) ? CxlRejReason.UNKNOWN_ORDER : CxlRejReason.OTHER);
        m.setString(Text.FIELD, reason == null || reason.isBlank() ? "REJECTED" : reason);
        return m;
    }

    private static void send(SessionID sessionId, Message message) {
        Session session = Session.lookupSession(sessionId);
        if (session != null) {
            session.send(message);
        }
    }
}
