package dev.matchbook.marketdata;

import dev.matchbook.proto.v1.BookImage;
import dev.matchbook.proto.v1.BookLevel;
import dev.matchbook.proto.v1.LevelUpdate;
import dev.matchbook.proto.v1.Side;
import dev.matchbook.proto.v1.Trade;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BookReplicaTest {

    private final BookReplica replica = new BookReplica(1);

    private static BookImage image(long seq) {
        return BookImage.newBuilder().setSymbolId(1).setSymbolSeq(seq)
                .addBids(BookLevel.newBuilder().setPrice(100).setQuantity(5).setOrderCount(1))
                .addBids(BookLevel.newBuilder().setPrice(99).setQuantity(7).setOrderCount(2))
                .addAsks(BookLevel.newBuilder().setPrice(101).setQuantity(3).setOrderCount(1))
                .build();
    }

    private static LevelUpdate level(long seq, Side side, long price, long qty) {
        return LevelUpdate.newBuilder().setSymbolId(1).setSymbolSeq(seq).setSide(side).setPrice(price)
                .setQuantity(qty).setOrderCount(qty == 0 ? 0 : 1).build();
    }

    @Test
    void ignoresIncrementalsUntilTheFirstImage() {
        assertThat(replica.apply(level(1, Side.SIDE_BUY, 100, 5))).isEqualTo(BookReplica.Applied.NOT_SYNCED);
        assertThat(replica.synced()).isFalse();
    }

    @Test
    void appliesContiguousUpdatesOnTopOfTheImage() {
        replica.reset(image(10));
        assertThat(replica.apply(level(11, Side.SIDE_BUY, 100, 0))).isEqualTo(BookReplica.Applied.APPLIED);
        assertThat(replica.apply(level(12, Side.SIDE_SELL, 102, 9))).isEqualTo(BookReplica.Applied.APPLIED);
        assertThat(replica.apply(Trade.newBuilder().setSymbolId(1).setSymbolSeq(13).setPrice(101).setQuantity(3)
                .setAggressorSide(Side.SIDE_BUY).build())).isEqualTo(BookReplica.Applied.APPLIED);

        assertThat(replica.bids(10)).extracting(BookReplica.Level::price).containsExactly(99L);
        assertThat(replica.asks(10)).extracting(BookReplica.Level::price).containsExactly(101L, 102L);
        assertThat(replica.recentTrades()).singleElement().extracting(BookReplica.TradePrint::aggressor).isEqualTo("BUY");
        assertThat(replica.seq()).isEqualTo(13);
    }

    @Test
    void bidsAreBestFirst() {
        replica.reset(image(1));
        replica.apply(level(2, Side.SIDE_BUY, 98, 1));
        replica.apply(level(3, Side.SIDE_BUY, 100, 9));
        assertThat(replica.bids(2)).extracting(BookReplica.Level::price).containsExactly(100L, 99L);
    }

    @Test
    void duplicatesAreIgnoredAndGapsInvalidateTheReplica() {
        replica.reset(image(10));
        replica.apply(level(11, Side.SIDE_BUY, 100, 1));
        assertThat(replica.apply(level(11, Side.SIDE_BUY, 100, 99))).isEqualTo(BookReplica.Applied.STALE);
        assertThat(replica.bids(1).getFirst().quantity()).isEqualTo(1);

        assertThat(replica.apply(level(13, Side.SIDE_BUY, 100, 2))).isEqualTo(BookReplica.Applied.GAP);
        assertThat(replica.synced()).isFalse();
        replica.reset(image(20));
        assertThat(replica.synced()).isTrue();
    }
}
