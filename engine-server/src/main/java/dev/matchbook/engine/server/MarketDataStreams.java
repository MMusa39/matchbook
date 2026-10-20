package dev.matchbook.engine.server;

import dev.matchbook.engine.core.Side;
import dev.matchbook.engine.pipeline.OutboundEvent;
import dev.matchbook.engine.pipeline.OutboundListener;
import dev.matchbook.proto.v1.BookImage;
import dev.matchbook.proto.v1.BookLevel;
import dev.matchbook.proto.v1.MarketDataEvent;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Public market data for one shard. A new subscriber first receives a {@link BookImage} per symbol,
 * taken by the matching thread at an exact point in the stream (so it is consistent with the
 * incrementals that follow), and only then that symbol's incrementals. Every message carries the
 * per-symbol sequence number, so the receiver can detect gaps.
 */
final class MarketDataStreams implements OutboundListener {

    /** A subscriber plus the symbols for which it has already received an image. */
    record Session(Subscriber<MarketDataEvent> subscriber, Set<Integer> synced) {
        Session(Subscriber<MarketDataEvent> subscriber) {
            this(subscriber, ConcurrentHashMap.newKeySet());
        }
    }

    private final int shard;
    private final List<Session> sessions;
    private BookImage.Builder image;

    MarketDataStreams(int shard, CopyOnWriteArrayList<Session> sessions) {
        this.shard = shard;
        this.sessions = sessions;
    }

    @Override
    public void onEvent(OutboundEvent event, boolean endOfBatch) {
        if (sessions.isEmpty()) {
            return;
        }
        switch (event.type()) {
            case BOOK_IMAGE_START -> image = BookImage.newBuilder()
                    .setSymbolId(event.symbolId()).setSymbolSeq(event.symbolSeq());
            case BOOK_IMAGE_LEVEL -> {
                BookLevel level = BookLevel.newBuilder().setPrice(event.price()).setQuantity(event.quantity())
                        .setOrderCount(event.orderCount()).build();
                if (event.side() == Side.BUY) {
                    image.addBids(level);
                } else {
                    image.addAsks(level);
                }
            }
            case BOOK_IMAGE_END -> {
                MarketDataEvent message = MarketDataEvent.newBuilder().setImage(image).build();
                image = null;
                for (Session s : sessions) {
                    if (s.synced().add(event.symbolId())) {
                        s.subscriber().offer(message);
                    }
                }
            }
            case BOOK_LEVEL -> publish(event.symbolId(),
                    MarketDataEvent.newBuilder().setLevel(ProtoMapping.level(event)).build());
            case TOP_OF_BOOK -> publish(event.symbolId(),
                    MarketDataEvent.newBuilder().setTop(ProtoMapping.top(event)).build());
            case TRADE -> publish(event.symbolId(),
                    MarketDataEvent.newBuilder().setTrade(ProtoMapping.trade(event, shard)).build());
            case EXECUTION_REPORT -> {
            }
        }
    }

    private void publish(int symbolId, MarketDataEvent message) {
        for (Session s : sessions) {
            if (s.synced().contains(symbolId)) {
                s.subscriber().offer(message);
            }
        }
    }
}
