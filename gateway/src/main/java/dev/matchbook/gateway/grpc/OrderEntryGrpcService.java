package dev.matchbook.gateway.grpc;

import dev.matchbook.gateway.order.ExecutionRouter;
import dev.matchbook.gateway.order.OrderOutcome;
import dev.matchbook.gateway.order.OrderRequest;
import dev.matchbook.gateway.order.OrderService;
import dev.matchbook.gateway.reference.InvalidRequestException;
import dev.matchbook.proto.v1.CancelOrderRequest;
import dev.matchbook.proto.v1.ClientExecutionReport;
import dev.matchbook.proto.v1.Fill;
import dev.matchbook.proto.v1.Liquidity;
import dev.matchbook.proto.v1.OrderEntryGrpc;
import dev.matchbook.proto.v1.OrderResponse;
import dev.matchbook.proto.v1.ReplaceOrderRequest;
import dev.matchbook.proto.v1.StreamExecutionsRequest;
import dev.matchbook.proto.v1.SubmitOrderRequest;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

/**
 * Public gRPC order entry. Business rejects (engine or risk) are normal responses with
 * {@code accepted = false}; infrastructure outcomes map to gRPC status codes: RESOURCE_EXHAUSTED (rate
 * limited), ALREADY_EXISTS (same client order id in flight), DEADLINE_EXCEEDED (outcome unknown),
 * UNAVAILABLE, INVALID_ARGUMENT.
 */
@Service
public class OrderEntryGrpcService extends OrderEntryGrpc.OrderEntryImplBase {

    private final OrderService orders;
    private final ExecutionRouter router;

    public OrderEntryGrpcService(OrderService orders, ExecutionRouter router) {
        this.orders = orders;
        this.router = router;
    }

    @Override
    public void submitOrder(SubmitOrderRequest r, StreamObserver<OrderResponse> observer) {
        handle(new OrderRequest.Submit(r.getClientOrderId(), r.getSymbol(), r.getSide(), r.getOrderType(),
                r.getTimeInForce(), r.getPrice(), r.getQuantity(), r.getStpMode()), observer);
    }

    @Override
    public void cancelOrder(CancelOrderRequest r, StreamObserver<OrderResponse> observer) {
        handle(new OrderRequest.Cancel(r.getClientOrderId(), r.getSymbol(), r.getOrderId()), observer);
    }

    @Override
    public void replaceOrder(ReplaceOrderRequest r, StreamObserver<OrderResponse> observer) {
        handle(new OrderRequest.Replace(r.getClientOrderId(), r.getSymbol(), r.getOrderId(), r.getPrice(),
                r.getQuantity()), observer);
    }

    @Override
    public void streamExecutions(StreamExecutionsRequest request, StreamObserver<ClientExecutionReport> observer) {
        long clientId = ApiKeyServerInterceptor.CLIENT_ID.get();
        ServerCallStreamObserver<ClientExecutionReport> call = (ServerCallStreamObserver<ClientExecutionReport>) observer;
        Runnable unsubscribe = router.subscribe(clientId, report -> {
            synchronized (call) {
                call.onNext(report);
            }
        });
        call.setOnCancelHandler(unsubscribe::run);
    }

    private void handle(OrderRequest request, StreamObserver<OrderResponse> observer) {
        long clientId = ApiKeyServerInterceptor.CLIENT_ID.get();
        OrderOutcome outcome;
        try {
            outcome = orders.handle(clientId, request);
        } catch (InvalidRequestException e) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        Status status = switch (outcome.result()) {
            case ACCEPTED, REJECTED -> Status.OK;
            case RATE_LIMITED -> Status.RESOURCE_EXHAUSTED.withDescription(outcome.reason());
            case IN_PROGRESS -> Status.ALREADY_EXISTS.withDescription("client order id is still being processed");
            case UNKNOWN -> Status.DEADLINE_EXCEEDED.withDescription("outcome unknown; check the execution stream");
            case UNAVAILABLE -> Status.UNAVAILABLE.withDescription(outcome.reason());
        };
        if (!status.isOk()) {
            observer.onError(status.asRuntimeException());
            return;
        }
        OrderResponse.Builder response = OrderResponse.newBuilder()
                .setAccepted(outcome.accepted())
                .setOrderId(outcome.orderId())
                .setStatus(outcome.status())
                .setRejectReason(outcome.accepted() ? "" : outcome.reason())
                .setFilledQuantity(outcome.filledQuantity())
                .setLeavesQuantity(outcome.leavesQuantity())
                .setReplayed(outcome.replayed());
        for (OrderOutcome.FillView f : outcome.fills()) {
            response.addFills(Fill.newBuilder().setTradeId(f.tradeId()).setPrice(f.price()).setQuantity(f.quantity())
                    .setLiquidity("MAKER".equals(f.liquidity()) ? Liquidity.LIQUIDITY_MAKER : Liquidity.LIQUIDITY_TAKER));
        }
        observer.onNext(response.build());
        observer.onCompleted();
    }
}
