package dev.matchbook.engine.pipeline;

import dev.matchbook.engine.core.Command;
import dev.matchbook.engine.core.CommandType;
import dev.matchbook.engine.core.OrderType;
import dev.matchbook.engine.core.Side;
import dev.matchbook.engine.core.StpMode;
import dev.matchbook.engine.core.TimeInForce;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/**
 * Fixed-layout binary encoding of a {@link Command}, used for journal records. Little endian, 72 bytes:
 * <pre>
 *  0 long journalSeq     8 long timestamp     16 long clientId   24 long clOrdId
 * 32 long price         40 long quantity      48 long orderId    56 int symbolId
 * 60 byte type  61 byte side  62 byte orderType  63 byte tif  64 byte stp  65..71 reserved
 * </pre>
 * Enum fields that are {@code null} (not used by the command type) are encoded as -1.
 */
public final class CommandCodec {

    public static final int LENGTH = 72;

    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;

    private CommandCodec() {
    }

    public static void encode(Command c, long journalSeq, MemorySegment segment, long offset) {
        segment.set(LONG, offset, journalSeq);
        segment.set(LONG, offset + 8, c.timestamp());
        segment.set(LONG, offset + 16, c.clientId());
        segment.set(LONG, offset + 24, c.clOrdId());
        segment.set(LONG, offset + 32, c.price());
        segment.set(LONG, offset + 40, c.quantity());
        segment.set(LONG, offset + 48, c.orderId());
        segment.set(INT, offset + 56, c.symbolId());
        segment.set(BYTE, offset + 60, (byte) c.type().code());
        segment.set(BYTE, offset + 61, code(c.side() == null ? -1 : c.side().code()));
        segment.set(BYTE, offset + 62, code(c.orderType() == null ? -1 : c.orderType().code()));
        segment.set(BYTE, offset + 63, code(c.timeInForce() == null ? -1 : c.timeInForce().code()));
        segment.set(BYTE, offset + 64, code(c.stpMode() == null ? -1 : c.stpMode().code()));
        segment.set(BYTE, offset + 65, (byte) 0);
        segment.set(INT, offset + 68, 0);
    }

    /** Decodes into {@code c} and returns the journal sequence number. */
    public static long decode(MemorySegment segment, long offset, Command c) {
        long journalSeq = segment.get(LONG, offset);
        long timestamp = segment.get(LONG, offset + 8);
        long clientId = segment.get(LONG, offset + 16);
        long clOrdId = segment.get(LONG, offset + 24);
        long price = segment.get(LONG, offset + 32);
        long quantity = segment.get(LONG, offset + 40);
        long orderId = segment.get(LONG, offset + 48);
        int symbolId = segment.get(INT, offset + 56);
        CommandType type = CommandType.fromCode(segment.get(BYTE, offset + 60));
        if (type == null) {
            throw new IllegalStateException("corrupt journal record: unknown command type at " + offset);
        }
        switch (type) {
            case NEW_ORDER -> c.newOrder(timestamp, clientId, clOrdId, symbolId,
                    Side.fromCode(segment.get(BYTE, offset + 61)),
                    OrderType.fromCode(segment.get(BYTE, offset + 62)),
                    TimeInForce.fromCode(segment.get(BYTE, offset + 63)),
                    StpMode.fromCode(segment.get(BYTE, offset + 64)),
                    price, quantity);
            case CANCEL -> c.cancel(timestamp, clientId, clOrdId, symbolId, orderId);
            case REPLACE -> c.replace(timestamp, clientId, clOrdId, symbolId, orderId, price, quantity);
        }
        return journalSeq;
    }

    private static byte code(int value) {
        return (byte) value;
    }
}
