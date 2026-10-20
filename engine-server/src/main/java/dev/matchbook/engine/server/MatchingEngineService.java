package dev.matchbook.engine.server;

import dev.matchbook.engine.core.Command;
import dev.matchbook.proto.v1.EngineCommand;
import dev.matchbook.proto.v1.EngineEvent;
import dev.matchbook.proto.v1.EngineResult;
import dev.matchbook.proto.v1.MarketDataEvent;
import dev.matchbook.proto.v1.MarketDataRequest;
import dev.matchbook.proto.v1.MatchingEngineGrpc;
import dev.matchbook.proto.v1.StreamEventsRequest;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * gRPC front of the engine. {@code Submit} copies the request into the shard's inbound ring and returns
 * immediately; the response is completed by {@link PendingResponses} on the shard's outbound listener
 * thread. A full ring is reported as {@code RESOURCE_EXHAUSTED} rather than blocking a gRPC thread.
 */
final class MatchingEngineService extends MatchingEngineGrpc.MatchingEngineImplBase {

    private static final int STREAM_QUEUE_CAPACITY = 262_144;

    private final EngineServer server;
    private final ThreadLocal<Command> commands = ThreadLocal.withInitial(Command::new);

    MatchingEngineService(EngineServer server) {
        this.server = server;
    }

    @Override
    public void submit(EngineCommand request, StreamObserver<EngineResult> observer) {
        long ingress = System.nanoTime();
        EngineServer.Shard shard = server.shardFor(ProtoMapping.symbolOf(request));
        if (shard == null) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription("unknown symbol").asRuntimeException());
            return;
        }
        Command command;
        try {
            command = ProtoMapping.toCommand(request, System.currentTimeMillis() * 1_000_000L, commands.get());
        } catch (IllegalArgumentException e) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        TraceContext.Parsed trace = TraceContext.parse(request.getTraceparent());
        long correlationId = shard.responses().register(observer, command.clientId());
        boolean published = trace == null
                ? shard.pipeline().tryPublish(command, correlationId, ingress, 0, 0, 0, (byte) 0)
                : shard.pipeline().tryPublish(command, correlationId, ingress, trace.traceIdHigh(),
                        trace.traceIdLow(), trace.spanId(), trace.flags());
        if (!published) {
            shard.responses().abandon(correlationId);
            observer.onError(Status.RESOURCE_EXHAUSTED.withDescription("engine busy").asRuntimeException());
        }
    }

    @Override
    public void streamEvents(StreamEventsRequest request, StreamObserver<EngineEvent> observer) {
        ServerCallStreamObserver<EngineEvent> call = (ServerCallStreamObserver<EngineEvent>) observer;
        String name = "events-" + (request.getSubscriber().isBlank() ? "anonymous" : request.getSubscriber());
        Subscriber<EngineEvent>[] holder = new Subscriber[1];
        holder[0] = new Subscriber<>(name, call, STREAM_QUEUE_CAPACITY, () -> server.eventSubscribers().remove(holder[0]));
        server.eventSubscribers().add(holder[0]);
    }

    @Override
    public void streamMarketData(MarketDataRequest request, StreamObserver<MarketDataEvent> observer) {
        ServerCallStreamObserver<MarketDataEvent> call = (ServerCallStreamObserver<MarketDataEvent>) observer;
        String name = "md-" + (request.getSubscriber().isBlank() ? "anonymous" : request.getSubscriber());
        MarketDataStreams.Session[] holder = new MarketDataStreams.Session[1];
        Subscriber<MarketDataEvent> subscriber = new Subscriber<>(name, call, STREAM_QUEUE_CAPACITY,
                () -> server.marketDataSessions().remove(holder[0]));
        holder[0] = new MarketDataStreams.Session(subscriber);
        server.marketDataSessions().add(holder[0]);
        // Images are taken after registration, so the session sees each image and every incremental after it.
        server.requestImages();
    }
}
