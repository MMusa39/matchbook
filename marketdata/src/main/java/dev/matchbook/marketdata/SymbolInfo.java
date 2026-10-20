package dev.matchbook.marketdata;

import java.math.BigDecimal;

/** Symbol reference data (same {@code symbols.json} as the engine and gateway). */
public record SymbolInfo(String symbol, int id, int shard, long priceScale, long tickSize, long lotSize,
                         long minQuantity, long maxQuantity, long lowPrice, long highPrice) {

    public String formatPrice(long units) {
        return BigDecimal.valueOf(units, Long.toString(priceScale).length() - 1).toPlainString();
    }
}
