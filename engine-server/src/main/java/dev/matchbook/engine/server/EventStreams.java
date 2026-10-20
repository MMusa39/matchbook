package dev.matchbook.engine.server;

import dev.matchbook.engine.pipeline.OutboundEvent;
import dev.matchbook.engine.pipeline.OutboundListener;
import dev.matchbook.proto.v1.EngineEvent;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Fans private execution reports and trades out to every {@code StreamEvents} subscriber (gateways). */
final class EventStreams implements OutboundListener {

    private final int shard;
    private final List<Subscriber<EngineEvent>> subscribers;

    EventStreams(int shard, CopyOnWriteArrayList<Subscriber<EngineEvent>> subscribers) {
        this.shard = shard;
        this.subscribers = subscribers;
    }

    @Override
    public void onEvent(OutboundEvent event, boolean endOfBatch) {
        if (subscribers.isEmpty()) {
            return;
        }
        EngineEvent message = switch (event.type()) {
            case EXECUTION_REPORT -> EngineEvent.newBuilder()
                    .setExecutionReport(ProtoMapping.executionReport(event, shard)).build();
            case TRADE -> EngineEvent.newBuilder().setTrade(ProtoMapping.trade(event, shard)).build();
            default -> null;
        };
        if (message != null) {
            for (Subscriber<EngineEvent> s : subscribers) {
                s.offer(message);
            }
        }
    }
}
