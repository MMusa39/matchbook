package dev.matchbook.engine.pipeline;

import dev.matchbook.engine.core.Command;
import dev.matchbook.engine.core.EngineConfig;
import dev.matchbook.engine.core.OrderType;
import dev.matchbook.engine.core.Side;
import dev.matchbook.engine.core.StpMode;
import dev.matchbook.engine.core.SymbolConfig;
import dev.matchbook.engine.core.TimeInForce;

import java.util.List;

/**
 * Deterministic, allocation-free order flow that keeps the book in a steady state, so a benchmark can
 * run for billions of commands without the book growing or draining.
 *
 * <p>Passive orders rest 1..{@value #MAX_OFFSET} ticks away from a fixed mid on their own side, so they
 * never cross each other. Their ids are tracked in a FIFO ring (the engine assigns ids sequentially, and
 * every order this generator sends is valid, so ids are predictable). When the ring is full the oldest
 * order is cancelled, which bounds the number of resting orders. Aggressive IOC orders cross a few ticks
 * and take liquidity. Cancels and replaces of orders that were already filled are rejected by the engine,
 * which is realistic traffic too.
 *
 * <p>Mix per command: 50% passive limit, 25% cancel, 10% replace (reprice, loses priority), 15%
 * aggressive IOC.
 */
public final class SyntheticOrderFlow {

    public static final int SYMBOL = 1;
    /** A second, otherwise empty symbol for benchmarks that must not interact with the steady-state book. */
    public static final int QUIET_SYMBOL = 2;
    public static final long MID = 100_000;
    public static final int MAX_OFFSET = 50;
    private static final int CLIENTS = 8;

    private final long[] ring;
    private final int mask;
    private int head;
    private int size;
    private long rng;
    private long nextOrderId = 1;
    private long clOrdId;

    /** @param maxResting ring capacity, rounded up to a power of two: the bound on resting orders */
    public SyntheticOrderFlow(long seed, int maxResting) {
        int capacity = Integer.highestOneBit(Math.max(2, maxResting - 1)) << 1;
        this.ring = new long[capacity];
        this.mask = capacity - 1;
        this.rng = seed == 0 ? 0x9E3779B97F4A7C15L : seed;
    }

    public static SymbolConfig symbol(int symbolId) {
        return new SymbolConfig(symbolId, "BENCH" + symbolId, 100, 1, 1, 1, 1_000_000, MID - 50_000, MID + 50_000);
    }

    public static EngineConfig engineConfig(int maxOrders, boolean pooling) {
        return new EngineConfig(0, maxOrders, List.of(symbol(SYMBOL), symbol(QUIET_SYMBOL)), pooling);
    }

    /** Fills {@code c} with the next command. */
    public Command next(Command c, long timestamp) {
        long client = 1 + (nextRandom() & 0x7FFF_FFFFL) % CLIENTS;
        int roll = (int) ((nextRandom() >>> 33) % 100);
        if (size == ring.length || (roll >= 50 && roll < 75 && size > 0)) {
            long entry = ring[head];
            head = (head + 1) & mask;
            size--;
            return c.cancel(timestamp, ownerOf(entry), ++clOrdId, SYMBOL, entry >>> 1);
        }
        if (roll >= 75 && roll < 85 && size > 0) {
            long entry = ring[(head + (int) ((nextRandom() >>> 33) % size)) & mask];
            Side side = (entry & 1) == 0 ? Side.BUY : Side.SELL;
            return c.replace(timestamp, ownerOf(entry), ++clOrdId, SYMBOL, entry >>> 1,
                    passivePrice(side), quantity());
        }
        if (roll >= 85) {
            Side side = (nextRandom() & 1) == 0 ? Side.BUY : Side.SELL;
            long cross = 1 + (nextRandom() >>> 33) % 5;
            long price = side == Side.BUY ? MID + cross : MID - cross;
            nextOrderId++;
            return c.newOrder(timestamp, client, ++clOrdId, SYMBOL, side, OrderType.LIMIT, TimeInForce.IOC,
                    StpMode.CANCEL_RESTING, price, 1 + (nextRandom() >>> 33) % 200);
        }
        Side side = (nextRandom() & 1) == 0 ? Side.BUY : Side.SELL;
        long id = nextOrderId++;
        ring[(head + size) & mask] = id << 1 | side.code();
        size++;
        owners[(int) (id & OWNER_MASK)] = (byte) client;
        return c.newOrder(timestamp, client, ++clOrdId, SYMBOL, side, OrderType.LIMIT, TimeInForce.GTC,
                StpMode.CANCEL_RESTING, passivePrice(side), quantity());
    }

    // Owner per order id, so cancels and replaces come from the right client. Indexed by the low bits of
    // the id; entries are overwritten long after the order has left the ring.
    private static final int OWNER_MASK = (1 << 22) - 1;
    private final byte[] owners = new byte[OWNER_MASK + 1];

    private long ownerOf(long entry) {
        return owners[(int) ((entry >>> 1) & OWNER_MASK)];
    }

    private long passivePrice(Side side) {
        long offset = 1 + (nextRandom() >>> 33) % MAX_OFFSET;
        return side == Side.BUY ? MID - offset : MID + offset;
    }

    private long quantity() {
        return 1 + (nextRandom() >>> 33) % 100;
    }

    /** xorshift64*: fast, allocation-free, deterministic for a given seed. */
    private long nextRandom() {
        long x = rng;
        x ^= x >>> 12;
        x ^= x << 25;
        x ^= x >>> 27;
        rng = x;
        return x * 0x2545F4914F6CDD1DL;
    }

    public int ringCapacity() {
        return ring.length;
    }
}
