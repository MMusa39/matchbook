package dev.matchbook.engine.core;

import java.util.List;

/**
 * Engine configuration.
 *
 * @param shardId    0..32767. Placed in the top bits of every order id and trade id so ids stay
 *                   unique across shards without coordination.
 * @param maxOrders  capacity of the order pool: the most live orders (resting plus the one being
 *                   matched) the engine holds. All of them are allocated up front.
 * @param symbols    the symbols this engine trades.
 * @param pooling    {@code true} (the default) recycles {@link Order} objects through a pre-allocated
 *                   pool. {@code false} allocates a new order per request and lets the GC reclaim it;
 *                   it exists only so benchmarks can measure what pooling buys.
 */
public record EngineConfig(int shardId, int maxOrders, List<SymbolConfig> symbols, boolean pooling) {

    public EngineConfig(int shardId, int maxOrders, List<SymbolConfig> symbols) {
        this(shardId, maxOrders, symbols, true);
    }

    public static final int SHARD_SHIFT = 48;

    public EngineConfig {
        if (shardId < 0 || shardId > Short.MAX_VALUE) {
            throw new IllegalArgumentException("shardId must be in 0..32767: " + shardId);
        }
        if (maxOrders <= 0) {
            throw new IllegalArgumentException("maxOrders must be > 0: " + maxOrders);
        }
        symbols = List.copyOf(symbols);
        if (symbols.isEmpty()) {
            throw new IllegalArgumentException("at least one symbol is required");
        }
        if (symbols.stream().map(SymbolConfig::symbolId).distinct().count() != symbols.size()) {
            throw new IllegalArgumentException("duplicate symbolId");
        }
    }
}
