package dev.matchbook.gateway.order;

import dev.matchbook.proto.v1.OrderType;
import dev.matchbook.proto.v1.Side;
import dev.matchbook.proto.v1.StpMode;
import dev.matchbook.proto.v1.TimeInForce;

/** A protocol-neutral order-entry request, built by the REST and gRPC front ends. */
public sealed interface OrderRequest {

    String clientOrderId();

    String symbol();

    record Submit(String clientOrderId, String symbol, Side side, OrderType orderType, TimeInForce timeInForce,
                  String price, String quantity, StpMode stpMode) implements OrderRequest {
    }

    record Cancel(String clientOrderId, String symbol, long orderId) implements OrderRequest {
    }

    record Replace(String clientOrderId, String symbol, long orderId, String price, String quantity)
            implements OrderRequest {
    }
}
