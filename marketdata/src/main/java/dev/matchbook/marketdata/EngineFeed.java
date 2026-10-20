package dev.matchbook.marketdata;

import dev.matchbook.proto.v1.MarketDataEvent;
import dev.matchbook.proto.v1.MarketDataRequest;
import dev.matchbook.proto.v1.MatchingEngineGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Subscribes to the engine's market data stream. Each (re)connection starts with a fresh book image per
 * symbol, so reconnecting is also how a replica that detected a gap resynchronises.
 */
@Component
public class EngineFeed {

    private static final Logger LOG = LoggerFactory.getLogger(EngineFeed.class);

    private final ManagedChannel channel;
    private final MarketDataHub hub;
    private final ScheduledExecutorService reconnect = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("md-feed-reconnect").factory());
    private volatile ClientCallStreamObserver<MarketDataRequest> current;
    private volatile boolean connected;
    private volatile boolean stopped;
    private long backoffMillis = 100;

    public EngineFeed(@Value("${matchbook.engine-target:localhost:9090}") String target, MarketDataHub hub) {
        this.channel = NettyChannelBuilder.forTarget(target).usePlaintext().keepAliveTime(30, TimeUnit.SECONDS).build();
        this.hub = hub;
        hub.onResyncNeeded(this::resync);
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
        MatchingEngineGrpc.newStub(channel).streamMarketData(
                MarketDataRequest.newBuilder().setSubscriber("marketdata").build(),
                new ClientResponseObserver<MarketDataRequest, MarketDataEvent>() {
                    @Override
                    public void beforeStart(ClientCallStreamObserver<MarketDataRequest> stream) {
                        current = stream;
                    }

                    @Override
                    public void onNext(MarketDataEvent event) {
                        if (!connected) {
                            connected = true;
                            backoffMillis = 100;
                            LOG.info("engine market data stream connected");
                        }
                        switch (event.getEventCase()) {
                            case IMAGE -> hub.onImage(event.getImage());
                            case LEVEL -> hub.onLevel(event.getLevel());
                            case TOP -> hub.onTop(event.getTop());
                            case TRADE -> hub.onTrade(event.getTrade());
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

    /** Drops the stream so the reconnect brings fresh images. */
    private void resync() {
        ClientCallStreamObserver<MarketDataRequest> stream = current;
        if (stream != null) {
            LOG.warn("market data gap detected; resubscribing for fresh images");
            stream.cancel("resync after gap", null);
        }
    }

    private void scheduleReconnect(String reason) {
        if (stopped) {
            return;
        }
        long delay = backoffMillis;
        backoffMillis = Math.min(5_000, backoffMillis * 2);
        LOG.warn("market data stream ended ({}); reconnecting in {} ms", reason, delay);
        reconnect.schedule(this::subscribe, delay, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    void stop() {
        stopped = true;
        reconnect.shutdownNow();
        channel.shutdownNow();
    }
}
