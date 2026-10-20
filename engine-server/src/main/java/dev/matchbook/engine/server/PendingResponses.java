package dev.matchbook.engine.server;

import dev.matchbook.engine.pipeline.OutboundEvent;
import dev.matchbook.engine.pipeline.OutboundListener;
import dev.matchbook.proto.v1.EngineResult;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Correlates unary {@code Submit} calls with the engine's output. The gRPC thread registers the call
 * under a fresh correlation id before publishing; this listener collects the command's events and
 * completes the call when the command's last event arrives.
 */
final class PendingResponses implements OutboundListener {

    private record Pending(StreamObserver<EngineResult> observer, long clientId, EngineResult.Builder result) {
    }

    private final AtomicLong correlationIds;
    private final ConcurrentHashMap<Long, Pending> pending = new ConcurrentHashMap<>();
    private final int shard;

    PendingResponses(int shard, AtomicLong correlationIds) {
        this.shard = shard;
        this.correlationIds = correlationIds;
    }

    long register(StreamObserver<EngineResult> observer, long clientId) {
        long id = correlationIds.incrementAndGet();
        pending.put(id, new Pending(observer, clientId, EngineResult.newBuilder()));
        return id;
    }

    void abandon(long correlationId) {
        pending.remove(correlationId);
    }

    int size() {
        return pending.size();
    }

    @Override
    public void onEvent(OutboundEvent event, boolean endOfBatch) {
        long id = event.correlationId();
        if (id == 0) {
            return;
        }
        Pending p = pending.get(id);
        if (p == null) {
            return;
        }
        switch (event.type()) {
            case EXECUTION_REPORT -> {
                // The sender's own reports; fills of other clients' resting orders go out on StreamEvents.
                if (event.clientId() == p.clientId()) {
                    p.result().addReports(ProtoMapping.executionReport(event, shard));
                }
            }
            case TRADE -> p.result().addTrades(ProtoMapping.trade(event, shard));
            default -> {
            }
        }
        if (event.lastOfCommand()) {
            pending.remove(id);
            p.result().setJournalSeq(event.journalSeq());
            p.observer().onNext(p.result().build());
            p.observer().onCompleted();
        }
    }
}
