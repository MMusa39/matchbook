package dev.matchbook.gateway.order;

import java.util.List;

/**
 * The gateway's answer to one order-entry request, shared by REST and gRPC, and stored verbatim (as JSON)
 * for idempotent replays.
 *
 * @param result   how the request ended, which decides the HTTP / gRPC status
 * @param accepted whether the engine accepted the request
 * @param status   order status after the request: NEW, PARTIALLY_FILLED, FILLED, CANCELED, REPLACED, REJECTED
 * @param reason   reject or cancel reason (engine reason such as PRICE_OUT_OF_BAND, or a gateway reason
 *                 such as RISK_FAT_FINGER)
 * @param replayed true when served from the idempotency store rather than processed now
 */
public record OrderOutcome(Result result, boolean accepted, long orderId, String clientOrderId, String symbol,
                           String status, String reason, String filledQuantity, String leavesQuantity,
                           List<FillView> fills, boolean replayed) {

    public enum Result {
        ACCEPTED,
        REJECTED,
        RATE_LIMITED,
        IN_PROGRESS,
        UNKNOWN,
        UNAVAILABLE
    }

    public record FillView(long tradeId, String price, String quantity, String liquidity) {
    }

    public OrderOutcome asReplay() {
        return new OrderOutcome(result, accepted, orderId, clientOrderId, symbol, status, reason, filledQuantity,
                leavesQuantity, fills, true);
    }

    static OrderOutcome failure(Result result, String clientOrderId, String symbol, String reason) {
        return new OrderOutcome(result, false, 0, clientOrderId, symbol, result.name(), reason, "0", "0", List.of(),
                false);
    }
}
