package dev.matchbook.engine.core;

/**
 * Static per-symbol trading parameters. Prices and quantities are fixed-point {@code long}s in the
 * symbol's minimum units; {@code priceScale} (for example 10_000 for four decimals) is only used to
 * convert at the edges of the system, never inside the engine.
 *
 * <p>{@code lowPrice} and {@code highPrice} are the static daily price band (limit-down / limit-up).
 * Orders priced outside it are rejected, which also bounds the price ladder that the book
 * pre-allocates: {@code (highPrice - lowPrice) / tickSize + 1} levels per side.
 */
public record SymbolConfig(
        int symbolId,
        String symbol,
        long priceScale,
        long tickSize,
        long lotSize,
        long minQuantity,
        long maxQuantity,
        long lowPrice,
        long highPrice) {

    /** Upper bound on ladder size per side, to keep pre-allocation reasonable. */
    public static final long MAX_LEVELS = 1L << 20;

    public SymbolConfig {
        if (symbolId < 0) {
            throw new IllegalArgumentException("symbolId must be >= 0: " + symbolId);
        }
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol must not be blank");
        }
        if (priceScale <= 0) {
            throw new IllegalArgumentException("priceScale must be > 0: " + priceScale);
        }
        if (tickSize <= 0 || lotSize <= 0) {
            throw new IllegalArgumentException("tickSize and lotSize must be > 0");
        }
        if (minQuantity <= 0 || maxQuantity < minQuantity) {
            throw new IllegalArgumentException("require 0 < minQuantity <= maxQuantity");
        }
        if (lowPrice <= 0 || highPrice < lowPrice) {
            throw new IllegalArgumentException("require 0 < lowPrice <= highPrice");
        }
        if (lowPrice % tickSize != 0 || highPrice % tickSize != 0) {
            throw new IllegalArgumentException("price band must be on the tick grid");
        }
        if ((highPrice - lowPrice) / tickSize + 1 > MAX_LEVELS) {
            throw new IllegalArgumentException("price band too wide: more than " + MAX_LEVELS + " levels");
        }
    }

    public int levelCount() {
        return (int) ((highPrice - lowPrice) / tickSize + 1);
    }
}
