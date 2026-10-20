package dev.matchbook.gateway.reference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SymbolTest {

    private final Symbol aapl = new Symbol("AAPL", 1, 0, 100, 1, 1, 1, 1_000_000, 10_000, 40_000);

    @Test
    void parsesDecimalPricesExactly() {
        assertThat(aapl.parsePrice("150.25")).isEqualTo(15_025);
        assertThat(aapl.parsePrice("150")).isEqualTo(15_000);
        assertThat(aapl.parsePrice("0.10")).isEqualTo(10);
        assertThat(aapl.parsePrice(" 99.9 ")).isEqualTo(9_990);
    }

    @ParameterizedTest
    @ValueSource(strings = {"150.255", "abc", "", "1e400"})
    void rejectsPricesItCannotRepresentInsteadOfRounding(String text) {
        assertThatThrownBy(() -> aapl.parsePrice(text)).isInstanceOf(InvalidRequestException.class);
    }

    @Test
    void quantitiesAreWholeShares() {
        assertThat(aapl.parseQuantity("100")).isEqualTo(100);
        assertThatThrownBy(() -> aapl.parseQuantity("1.5")).isInstanceOf(InvalidRequestException.class);
    }

    @Test
    void formatsWithTheSymbolsDecimals() {
        assertThat(aapl.formatPrice(15_025)).isEqualTo("150.25");
        assertThat(aapl.formatPrice(15_000)).isEqualTo("150.00");
    }
}
