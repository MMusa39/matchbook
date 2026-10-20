package dev.matchbook.gateway.risk;

import dev.matchbook.gateway.config.GatewayProperties;
import dev.matchbook.gateway.reference.Symbol;
import dev.matchbook.proto.v1.ExecType;
import dev.matchbook.proto.v1.ExecutionReport;
import dev.matchbook.proto.v1.OrderType;
import dev.matchbook.proto.v1.Side;
import dev.matchbook.proto.v1.Trade;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pre-trade risk, in memory, fed by the engine's execution stream.
 *
 * <p>Checks, in order: maximum order size, maximum notional (price × quantity; market orders are valued
 * at the far edge of the symbol's price band, the worst case), fat-finger distance from the last trade,
 * and the position limit. The position check is conservative: it counts every open order as if it will
 * fill, plus orders admitted but not yet acknowledged by the engine (reservations). Two concurrent
 * orders from one client therefore cannot both slip under the limit.
 *
 * <p>State is per gateway instance; with several gateways, clients must be routed to a fixed instance
 * (or the state moved to a shared store). See DESIGN.md.
 */
@Component
public class RiskManager {

    /** Exposure held for an order between admission and the engine's response. */
    public record Reservation(long clientId, int symbolId, Side side, long quantity) {
    }

    private static final class Book {
        long position;          // filled: bought minus sold
        long openBuy;           // leaves of resting buys, plus reserved buys
        long openSell;
        final Map<Long, Long> openOrders = new HashMap<>(); // orderId -> signed leaves
    }

    private final GatewayProperties.Risk limits;
    private final Map<Long, Map<Integer, Book>> clients = new ConcurrentHashMap<>();
    private final Map<Integer, Long> lastTradePrice = new ConcurrentHashMap<>();

    public RiskManager(GatewayProperties properties) {
        this.limits = properties.risk();
    }

    public RiskCheck checkNewOrder(long clientId, Symbol symbol, Side side, OrderType type, long price, long quantity) {
        if (quantity > limits.maxOrderQuantity()) {
            return RiskCheck.reject("RISK_MAX_ORDER_QUANTITY");
        }
        long valuationPrice = type == OrderType.ORDER_TYPE_MARKET
                ? (side == Side.SIDE_BUY ? symbol.highPrice() : symbol.lowPrice())
                : price;
        long notionalUnits;
        try {
            notionalUnits = Math.multiplyExact(valuationPrice, quantity);
        } catch (ArithmeticException e) {
            return RiskCheck.reject("RISK_MAX_NOTIONAL");
        }
        if (symbol.toDecimal(notionalUnits).compareTo(limits.maxNotional()) > 0) {
            return RiskCheck.reject("RISK_MAX_NOTIONAL");
        }
        Long last = lastTradePrice.get(symbol.id());
        if (type == OrderType.ORDER_TYPE_LIMIT && last != null && isFatFinger(price, last)) {
            return RiskCheck.reject("RISK_FAT_FINGER");
        }
        Book book = book(clientId, symbol.id());
        synchronized (book) {
            if (side == Side.SIDE_BUY) {
                if (book.position + book.openBuy + quantity > limits.maxPosition()) {
                    return RiskCheck.reject("RISK_POSITION_LIMIT");
                }
                book.openBuy += quantity;
            } else {
                if (book.position - book.openSell - quantity < -limits.maxPosition()) {
                    return RiskCheck.reject("RISK_POSITION_LIMIT");
                }
                book.openSell += quantity;
            }
        }
        return new RiskCheck(null, new Reservation(clientId, symbol.id(), side, quantity));
    }

    private boolean isFatFinger(long price, long last) {
        BigDecimal distance = BigDecimal.valueOf(Math.abs(price - last)).multiply(BigDecimal.valueOf(100))
                .divide(BigDecimal.valueOf(last), 6, RoundingMode.HALF_UP);
        return distance.compareTo(limits.fatFingerPercent()) > 0;
    }

    /**
     * Releases a reservation once the engine has answered. The real open exposure is tracked from the
     * execution reports themselves ({@link #onExecutionReport}), which arrive on the event stream.
     */
    public void release(Reservation reservation) {
        if (reservation == null) {
            return;
        }
        Book book = book(reservation.clientId(), reservation.symbolId());
        synchronized (book) {
            if (reservation.side() == Side.SIDE_BUY) {
                book.openBuy -= reservation.quantity();
            } else {
                book.openSell -= reservation.quantity();
            }
        }
    }

    /** Applies an engine execution report to positions and open orders. */
    public void onExecutionReport(ExecutionReport r) {
        Book book = book(r.getClientId(), r.getSymbolId());
        synchronized (book) {
            Long previous = book.openOrders.remove(r.getOrderId());
            if (previous != null) {
                if (previous > 0) {
                    book.openBuy -= previous;
                } else {
                    book.openSell += previous;
                }
            }
            if (r.getExecType() == ExecType.EXEC_TYPE_PARTIAL_FILL || r.getExecType() == ExecType.EXEC_TYPE_FILL) {
                book.position += r.getSide() == Side.SIDE_BUY ? r.getLastQuantity() : -r.getLastQuantity();
            }
            boolean live = r.getLeavesQuantity() > 0 && switch (r.getExecType()) {
                case EXEC_TYPE_NEW, EXEC_TYPE_PARTIAL_FILL, EXEC_TYPE_REPLACED -> true;
                default -> false;
            };
            if (live) {
                long signed = r.getSide() == Side.SIDE_BUY ? r.getLeavesQuantity() : -r.getLeavesQuantity();
                book.openOrders.put(r.getOrderId(), signed);
                if (signed > 0) {
                    book.openBuy += signed;
                } else {
                    book.openSell -= signed;
                }
            }
        }
    }

    public void onTrade(Trade trade) {
        lastTradePrice.put(trade.getSymbolId(), trade.getPrice());
    }

    public long position(long clientId, int symbolId) {
        Book book = book(clientId, symbolId);
        synchronized (book) {
            return book.position;
        }
    }

    public long openBuy(long clientId, int symbolId) {
        Book book = book(clientId, symbolId);
        synchronized (book) {
            return book.openBuy;
        }
    }

    public long openSell(long clientId, int symbolId) {
        Book book = book(clientId, symbolId);
        synchronized (book) {
            return book.openSell;
        }
    }

    private Book book(long clientId, int symbolId) {
        return clients.computeIfAbsent(clientId, c -> new ConcurrentHashMap<>()).computeIfAbsent(symbolId, s -> new Book());
    }
}
