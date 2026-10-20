package dev.matchbook.engine.core;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Binary snapshot of the complete engine state: counters, per-symbol market data state and every
 * resting order in queue order.
 *
 * <p>The encoding is canonical: books in symbol-id order, bids best to worst then asks best to
 * worst, each level oldest first. Two engines in the same state therefore produce byte-identical
 * snapshots, which is what the recovery tests compare.
 *
 * <p>Layout (little endian):
 * <pre>
 * int magic, int version, int shardId, int bookCount,
 * long seq, long orderCounter, long tradeCounter,
 * per book: int symbolId, long symbolSeq, long bidPx, long bidQty, long askPx, long askQty, int orderCount,
 *           per order: 9 longs (id, client, clOrdId, price, qty, leaves, cum, ts, reserved) + 4 bytes
 *           (side, type, tif, stp)
 * </pre>
 * Uses plain {@link ByteBuffer} absolute access (heap or direct, any byte order is overridden by the
 * explicit little-endian views) so the core needs no {@code Unsafe} access or extra JVM flags.
 * Snapshots are taken on the matching thread and may allocate; they are not on the per-order path.
 */
public final class EngineSnapshot {

    public static final int MAGIC = 0x4D42534E; // "MBSN"
    public static final int VERSION = 1;
    private static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;
    private static final int ORDER_RECORD_LENGTH = 9 * Long.BYTES + 4;

    private EngineSnapshot() {
    }

    /** Upper bound on the encoded size for the engine's current state. */
    public static int maxLength(MatchingEngine engine) {
        int length = 4 * Integer.BYTES + 3 * Long.BYTES;
        for (OrderBook book : engine.books()) {
            if (book != null) {
                length += 2 * Integer.BYTES + 5 * Long.BYTES + book.restingOrders() * ORDER_RECORD_LENGTH;
            }
        }
        return length;
    }

    /** Writes the snapshot at {@code offset} and returns the number of bytes written. */
    public static int write(MatchingEngine engine, ByteBuffer target, int offset) {
        ByteBuffer buffer = target.duplicate().order(ORDER);
        int p = offset;
        OrderBook[] books = engine.books();
        int bookCount = 0;
        for (OrderBook book : books) {
            if (book != null) {
                bookCount++;
            }
        }
        buffer.putInt(p, MAGIC);
        p += Integer.BYTES;
        buffer.putInt(p, VERSION);
        p += Integer.BYTES;
        buffer.putInt(p, engine.config().shardId());
        p += Integer.BYTES;
        buffer.putInt(p, bookCount);
        p += Integer.BYTES;
        buffer.putLong(p, engine.lastSeq());
        p += Long.BYTES;
        buffer.putLong(p, engine.orderCounter());
        p += Long.BYTES;
        buffer.putLong(p, engine.tradeCounter());
        p += Long.BYTES;

        for (OrderBook book : books) {
            if (book == null) {
                continue;
            }
            buffer.putInt(p, book.config().symbolId());
            p += Integer.BYTES;
            buffer.putLong(p, book.symbolSeq);
            p += Long.BYTES;
            buffer.putLong(p, book.publishedBidPrice);
            p += Long.BYTES;
            buffer.putLong(p, book.publishedBidQuantity);
            p += Long.BYTES;
            buffer.putLong(p, book.publishedAskPrice);
            p += Long.BYTES;
            buffer.putLong(p, book.publishedAskQuantity);
            p += Long.BYTES;
            buffer.putInt(p, book.restingOrders());
            p += Integer.BYTES;
            p = writeSide(book, Side.BUY, buffer, p);
            p = writeSide(book, Side.SELL, buffer, p);
        }
        return p - offset;
    }

    private static int writeSide(OrderBook book, Side side, ByteBuffer buffer, int p) {
        for (PriceLevel level = book.bestLevel(side); level != null; level = book.nextLevel(side, level)) {
            for (Order o = level.head; o != null; o = o.next) {
                buffer.putLong(p, o.orderId);
                buffer.putLong(p + 8, o.clientId);
                buffer.putLong(p + 16, o.clOrdId);
                buffer.putLong(p + 24, o.price);
                buffer.putLong(p + 32, o.quantity);
                buffer.putLong(p + 40, o.leavesQuantity);
                buffer.putLong(p + 48, o.cumQuantity);
                buffer.putLong(p + 56, o.timestamp);
                buffer.putLong(p + 64, 0L);
                buffer.put(p + 72, (byte) o.side.code());
                buffer.put(p + 73, (byte) o.orderType.code());
                buffer.put(p + 74, (byte) o.timeInForce.code());
                buffer.put(p + 75, (byte) o.stpMode.code());
                p += ORDER_RECORD_LENGTH;
            }
        }
        return p;
    }

    /**
     * Loads a snapshot into a freshly constructed engine with the same configuration.
     *
     * @throws IllegalStateException if the engine is not fresh or the snapshot does not match its config
     */
    public static void read(MatchingEngine engine, ByteBuffer source, int offset, int length) {
        ByteBuffer buffer = source.duplicate().order(ORDER);
        if (!engine.isEmpty()) {
            throw new IllegalStateException("snapshot can only be loaded into a fresh engine");
        }
        int p = offset;
        int end = offset + length;
        if (buffer.getInt(p) != MAGIC) {
            throw new IllegalStateException("not an engine snapshot");
        }
        p += Integer.BYTES;
        int version = buffer.getInt(p);
        if (version != VERSION) {
            throw new IllegalStateException("unsupported snapshot version " + version);
        }
        p += Integer.BYTES;
        int shardId = buffer.getInt(p);
        if (shardId != engine.config().shardId()) {
            throw new IllegalStateException("snapshot is for shard " + shardId);
        }
        p += Integer.BYTES;
        int bookCount = buffer.getInt(p);
        p += Integer.BYTES;
        long seq = buffer.getLong(p);
        p += Long.BYTES;
        long orderCounter = buffer.getLong(p);
        p += Long.BYTES;
        long tradeCounter = buffer.getLong(p);
        p += Long.BYTES;

        Order template = new Order();
        for (int b = 0; b < bookCount; b++) {
            int symbolId = buffer.getInt(p);
            p += Integer.BYTES;
            OrderBook book = engine.orderBook(symbolId);
            if (book == null) {
                throw new IllegalStateException("snapshot has unknown symbol " + symbolId);
            }
            book.symbolSeq = buffer.getLong(p);
            book.publishedBidPrice = buffer.getLong(p + 8);
            book.publishedBidQuantity = buffer.getLong(p + 16);
            book.publishedAskPrice = buffer.getLong(p + 24);
            book.publishedAskQuantity = buffer.getLong(p + 32);
            p += 5 * Long.BYTES;
            int orderCount = buffer.getInt(p);
            p += Integer.BYTES;
            for (int i = 0; i < orderCount; i++) {
                template.orderId = buffer.getLong(p);
                template.clientId = buffer.getLong(p + 8);
                template.clOrdId = buffer.getLong(p + 16);
                template.price = buffer.getLong(p + 24);
                template.quantity = buffer.getLong(p + 32);
                template.leavesQuantity = buffer.getLong(p + 40);
                template.cumQuantity = buffer.getLong(p + 48);
                template.timestamp = buffer.getLong(p + 56);
                template.side = Side.fromCode(buffer.get(p + 72));
                template.orderType = OrderType.fromCode(buffer.get(p + 73));
                template.timeInForce = TimeInForce.fromCode(buffer.get(p + 74));
                template.stpMode = StpMode.fromCode(buffer.get(p + 75));
                template.symbolId = symbolId;
                engine.restoreRestingOrder(template);
                p += ORDER_RECORD_LENGTH;
            }
        }
        if (p != end) {
            throw new IllegalStateException("snapshot length mismatch: read " + (p - offset) + " of " + length);
        }
        engine.restoreCounters(seq, orderCounter, tradeCounter);
    }
}
