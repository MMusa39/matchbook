package dev.matchbook.gateway.engine;

import dev.matchbook.gateway.order.ExecutionRouter;
import dev.matchbook.gateway.risk.RiskManager;
import dev.matchbook.proto.v1.EngineEvent;
import dev.matchbook.proto.v1.MatchingEngineGrpc;
import dev.matchbook.proto.v1.StreamEventsRequest;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Subscribes to the engine's private event stream (all execution reports and trades) and feeds risk
 * state and client execution streams. Reconnects with capped exponential backoff when the stream drops.
 */
@Component
public class EngineEventSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(EngineEventSubscriber.class);

    private final EngineClient engine;
    private final RiskManager risk;
    private final ExecutionRouter router;
    private final ScheduledExecutorService reconnect = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("engine-events-reconnect").factory());
    private volatile boolean connected;
    private volatile boolean stopped;
    private long backoffMillis = 100;

    public EngineEventSubscriber(EngineClient engine, RiskManager risk, ExecutionRouter router) {
        this.engine = engine;
        this.risk = risk;
        this.router = router;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        subscribe();
    }

    public boolean isConnected() {
        return connected;
    }

    private void subscribe() {
        if (stopped) {
            return;
        }
        MatchingEngineGrpc.newStub(engine.channel()).streamEvents(
                StreamEventsRequest.newBuilder().setSubscriber("gateway").build(), new StreamObserver<>() {
                    @Override
                    public void onNext(EngineEvent event) {
                        if (!connected) {
                            connected = true;
                            backoffMillis = 100;
                            LOG.info("engine event stream connected");
                        }
                        switch (event.getEventCase()) {
                            case EXECUTION_REPORT -> {
                                risk.onExecutionReport(event.getExecutionReport());
                                router.route(event.getExecutionReport());
                            }
                            case TRADE -> risk.onTrade(event.getTrade());
                            case EVENT_NOT_SET -> {
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        connected = false;
                        scheduleReconnect(t.toString());
                    }

                    @Override
                    public void onCompleted() {
                        connected = false;
                        scheduleReconnect("completed");
                    }
                });
    }

    private void scheduleReconnect(String reason) {
        if (stopped) {
            return;
        }
        long delay = backoffMillis;
        backoffMillis = Math.min(5_000, backoffMillis * 2);
        LOG.warn("engine event stream ended ({}); reconnecting in {} ms", reason, delay);
        reconnect.schedule(this::subscribe, delay, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    void stop() {
        stopped = true;
        reconnect.shutdownNow();
    }
}
