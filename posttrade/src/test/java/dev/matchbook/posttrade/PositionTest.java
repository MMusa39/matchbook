package dev.matchbook.posttrade;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class PositionTest {

    @Test
    void openingAndAddingAveragesTheCost() {
        Position p = Position.flat().apply(true, 100, 10_000).apply(true, 100, 11_000);
        assertThat(p.quantity()).isEqualTo(200);
        assertThat(p.avgPrice()).isEqualByComparingTo("10500");
        assertThat(p.realizedPnl()).isEqualByComparingTo("0");
        assertThat(p.bought()).isEqualTo(200);
    }

    @Test
    void reducingRealisesAgainstTheAverageAndKeepsIt() {
        Position p = Position.flat().apply(true, 200, 10_500).apply(false, 50, 12_000);
        assertThat(p.quantity()).isEqualTo(150);
        assertThat(p.avgPrice()).isEqualByComparingTo("10500");
        assertThat(p.realizedPnl()).isEqualByComparingTo(BigDecimal.valueOf(50 * 1_500));
    }

    @Test
    void closingResetsTheAverage() {
        Position p = Position.flat().apply(true, 10, 10_000).apply(false, 10, 9_000);
        assertThat(p.quantity()).isZero();
        assertThat(p.avgPrice()).isEqualByComparingTo("0");
        assertThat(p.realizedPnl()).isEqualByComparingTo("-10000");
    }

    @Test
    void flippingClosesThenOpensTheRemainderAtTheFillPrice() {
        Position p = Position.flat().apply(true, 10, 10_000).apply(false, 25, 10_400);
        assertThat(p.quantity()).isEqualTo(-15);
        assertThat(p.avgPrice()).isEqualByComparingTo("10400");
        assertThat(p.realizedPnl()).isEqualByComparingTo("4000");
    }

    @Test
    void shortsProfitWhenThePriceFalls() {
        Position p = Position.flat().apply(false, 20, 10_000).apply(true, 5, 9_000);
        assertThat(p.quantity()).isEqualTo(-15);
        assertThat(p.avgPrice()).isEqualByComparingTo("10000");
        assertThat(p.realizedPnl()).isEqualByComparingTo("5000");
        assertThat(p.sold()).isEqualTo(20);
        assertThat(p.bought()).isEqualTo(5);
    }

    @Test
    void averagesCanBeFractional() {
        Position p = Position.flat().apply(true, 3, 100).apply(true, 1, 101);
        assertThat(p.avgPrice()).isEqualByComparingTo("100.25");
    }
}
