package dev.matchbook.engine.server;

import dev.matchbook.engine.core.SymbolConfig;

/**
 * One tradable symbol as defined in {@code symbols.json}, the reference data shared by every service.
 * Prices are fixed-point in units of {@code 1 / priceScale}; for example with priceScale 100, a price of
 * 15025 means 150.25.
 */
public record SymbolDefinition(String symbol, int id, int shard, long priceScale, long tickSize, long lotSize,
                               long minQuantity, long maxQuantity, long lowPrice, long highPrice) {

    public SymbolConfig toEngineConfig() {
        return new SymbolConfig(id, symbol, priceScale, tickSize, lotSize, minQuantity, maxQuantity, lowPrice,
                highPrice);
    }
}
