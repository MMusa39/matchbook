package dev.matchbook.gateway.api;

import dev.matchbook.gateway.order.OrderOutcome;
import dev.matchbook.gateway.order.OrderRequest;
import dev.matchbook.gateway.order.OrderService;
import dev.matchbook.gateway.reference.InvalidRequestException;
import dev.matchbook.gateway.reference.Symbol;
import dev.matchbook.gateway.reference.SymbolRegistry;
import dev.matchbook.gateway.risk.RiskManager;
import dev.matchbook.proto.v1.OrderType;
import dev.matchbook.proto.v1.Side;
import dev.matchbook.proto.v1.StpMode;
import dev.matchbook.proto.v1.TimeInForce;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Convenience REST API over the same order pipeline as gRPC. Status codes: 201 a new order accepted,
 * 200 a cancel/replace accepted or an idempotent replay, 422 rejected (engine or risk), 429 rate limited,
 * 409 the same client order id is still in flight, 504 outcome unknown (engine timeout), 503 unavailable.
 */
@RestController
@RequestMapping("/api/v1")
public class OrderController {

    public record SubmitBody(String clientOrderId, String symbol, String side, String type, String timeInForce,
                             String price, String quantity, String stp) {
    }

    public record CancelBody(String clientOrderId, String symbol) {
    }

    public record ReplaceBody(String clientOrderId, String symbol, String price, String quantity) {
    }

    public record PositionView(String symbol, long position, long openBuy, long openSell) {
    }

    private final OrderService orders;
    private final SymbolRegistry symbols;
    private final RiskManager risk;

    public OrderController(OrderService orders, SymbolRegistry symbols, RiskManager risk) {
        this.orders = orders;
        this.symbols = symbols;
        this.risk = risk;
    }

    @PostMapping("/orders")
    public ResponseEntity<OrderOutcome> submit(@RequestAttribute(ApiKeyInterceptor.CLIENT_ID) long clientId,
                                               @RequestBody SubmitBody body) {
        OrderRequest.Submit request = new OrderRequest.Submit(body.clientOrderId(), body.symbol(),
                parse(body.side(), Map.of("BUY", Side.SIDE_BUY, "SELL", Side.SIDE_SELL), "side"),
                parse(body.type() == null ? "LIMIT" : body.type(),
                        Map.of("LIMIT", OrderType.ORDER_TYPE_LIMIT, "MARKET", OrderType.ORDER_TYPE_MARKET), "type"),
                body.timeInForce() == null ? null : parse(body.timeInForce(), Map.of(
                        "GTC", TimeInForce.TIME_IN_FORCE_GTC, "IOC", TimeInForce.TIME_IN_FORCE_IOC,
                        "FOK", TimeInForce.TIME_IN_FORCE_FOK), "timeInForce"),
                body.price(), body.quantity(),
                body.stp() == null ? null : parse(body.stp(), Map.of(
                        "CANCEL_RESTING", StpMode.STP_MODE_CANCEL_RESTING,
                        "CANCEL_AGGRESSOR", StpMode.STP_MODE_CANCEL_AGGRESSOR,
                        "CANCEL_BOTH", StpMode.STP_MODE_CANCEL_BOTH), "stp"));
        return respond(orders.handle(clientId, request), HttpStatus.CREATED);
    }

    @PostMapping("/orders/{orderId}/cancel")
    public ResponseEntity<OrderOutcome> cancel(@RequestAttribute(ApiKeyInterceptor.CLIENT_ID) long clientId,
                                               @PathVariable long orderId, @RequestBody CancelBody body) {
        return respond(orders.handle(clientId, new OrderRequest.Cancel(body.clientOrderId(), body.symbol(), orderId)),
                HttpStatus.OK);
    }

    @PostMapping("/orders/{orderId}/replace")
    public ResponseEntity<OrderOutcome> replace(@RequestAttribute(ApiKeyInterceptor.CLIENT_ID) long clientId,
                                                @PathVariable long orderId, @RequestBody ReplaceBody body) {
        return respond(orders.handle(clientId, new OrderRequest.Replace(body.clientOrderId(), body.symbol(), orderId,
                body.price(), body.quantity())), HttpStatus.OK);
    }

    @GetMapping("/symbols")
    public List<Symbol> symbols() {
        return symbols.all();
    }

    /** The gateway's live risk view of the caller's positions (filled) and open exposure. */
    @GetMapping("/positions")
    public List<PositionView> positions(@RequestAttribute(ApiKeyInterceptor.CLIENT_ID) long clientId) {
        return symbols.all().stream()
                .map(s -> new PositionView(s.symbol(), risk.position(clientId, s.id()),
                        risk.openBuy(clientId, s.id()), risk.openSell(clientId, s.id())))
                .toList();
    }

    static ResponseEntity<OrderOutcome> respond(OrderOutcome outcome, HttpStatus acceptedStatus) {
        HttpStatus status = switch (outcome.result()) {
            case ACCEPTED -> outcome.replayed() ? HttpStatus.OK : acceptedStatus;
            case REJECTED -> HttpStatus.UNPROCESSABLE_CONTENT;
            case RATE_LIMITED -> HttpStatus.TOO_MANY_REQUESTS;
            case IN_PROGRESS -> HttpStatus.CONFLICT;
            case UNKNOWN -> HttpStatus.GATEWAY_TIMEOUT;
            case UNAVAILABLE -> HttpStatus.SERVICE_UNAVAILABLE;
        };
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(status);
        if (outcome.result() == OrderOutcome.Result.RATE_LIMITED) {
            long millis = Long.parseLong(outcome.reason().substring(outcome.reason().indexOf(':') + 1));
            builder.header("Retry-After", Long.toString(Math.max(1, (millis + 999) / 1000)));
        }
        return builder.body(outcome);
    }

    private static <T> T parse(String value, Map<String, T> allowed, String field) {
        T parsed = value == null ? null : allowed.get(value.toUpperCase());
        if (parsed == null) {
            throw new InvalidRequestException(field + " must be one of " + allowed.keySet());
        }
        return parsed;
    }
}
