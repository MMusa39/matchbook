package dev.matchbook.posttrade;

import java.math.BigDecimal;
import java.math.MathContext;

/**
 * A client's net position in one symbol, valued with the average-cost method. Prices are in the symbol's
 * fixed-point units; {@code avgPrice} and {@code realizedPnl} may be fractional.
 *
 * <ul>
 *     <li>A fill that adds to the position (or opens one) moves the average price to the quantity-weighted
 *     average of the old position and the fill.</li>
 *     <li>A fill that reduces the position realises {@code closed × (fill price − average price)} for a
 *     long (the reverse for a short) and leaves the average price unchanged.</li>
 *     <li>A fill that flips the position closes the old one completely, then opens the remainder at the
 *     fill price.</li>
 * </ul>
 */
public record Position(long quantity, BigDecimal avgPrice, BigDecimal realizedPnl, long bought, long sold) {

    private static final MathContext MC = MathContext.DECIMAL128;

    public static Position flat() {
        return new Position(0, BigDecimal.ZERO, BigDecimal.ZERO, 0, 0);
    }

    /** Applies a fill: {@code buy} true for a purchase, {@code quantity} > 0. */
    public Position apply(boolean buy, long quantity, long price) {
        long signed = buy ? quantity : -quantity;
        long newBought = bought + (buy ? quantity : 0);
        long newSold = sold + (buy ? 0 : quantity);
        BigDecimal fillPrice = BigDecimal.valueOf(price);

        if (this.quantity == 0 || Long.signum(this.quantity) == Long.signum(signed)) {
            long total = Math.addExact(this.quantity, signed);
            BigDecimal weighted = avgPrice.multiply(BigDecimal.valueOf(Math.abs(this.quantity)), MC)
                    .add(fillPrice.multiply(BigDecimal.valueOf(quantity), MC), MC);
            BigDecimal avg = weighted.divide(BigDecimal.valueOf(Math.abs(total)), MC);
            return new Position(total, avg, realizedPnl, newBought, newSold);
        }

        long closed = Math.min(quantity, Math.abs(this.quantity));
        BigDecimal perShare = this.quantity > 0 ? fillPrice.subtract(avgPrice, MC) : avgPrice.subtract(fillPrice, MC);
        BigDecimal realized = realizedPnl.add(perShare.multiply(BigDecimal.valueOf(closed), MC), MC);
        long remaining = Math.addExact(this.quantity, signed);
        BigDecimal avg;
        if (remaining == 0) {
            avg = BigDecimal.ZERO;
        } else if (Long.signum(remaining) == Long.signum(this.quantity)) {
            avg = avgPrice;            // reduced, not flipped
        } else {
            avg = fillPrice;           // flipped: the remainder opens at the fill price
        }
        return new Position(remaining, avg, realized, newBought, newSold);
    }
}
