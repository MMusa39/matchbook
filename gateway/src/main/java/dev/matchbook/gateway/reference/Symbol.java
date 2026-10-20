package dev.matchbook.gateway.reference;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Symbol reference data, as in {@code symbols.json}. Prices are fixed-point in units of
 * {@code 1 / priceScale}; {@code priceScale} must be a power of ten. Quantities are whole shares.
 */
public record Symbol(String symbol, int id, int shard, long priceScale, long tickSize, long lotSize,
                     long minQuantity, long maxQuantity, long lowPrice, long highPrice) {

    public int priceDecimals() {
        return Long.toString(priceScale).length() - 1;
    }

    /**
     * Parses a decimal price such as {@code "150.25"} into fixed-point units. Rejects more decimal places
     * than the symbol supports instead of rounding: silently rounding a price is how fat-finger errors
     * become trades.
     */
    public long parsePrice(String text) {
        BigDecimal value = parseDecimal(text, "price");
        try {
            return value.movePointRight(priceDecimals()).setScale(0, RoundingMode.UNNECESSARY).longValueExact();
        } catch (ArithmeticException e) {
            throw new InvalidRequestException("price " + text + " has more than " + priceDecimals()
                    + " decimal places or is out of range");
        }
    }

    public long parseQuantity(String text) {
        BigDecimal value = parseDecimal(text, "quantity");
        try {
            return value.setScale(0, RoundingMode.UNNECESSARY).longValueExact();
        } catch (ArithmeticException e) {
            throw new InvalidRequestException("quantity " + text + " must be a whole number of shares");
        }
    }

    public String formatPrice(long units) {
        return BigDecimal.valueOf(units, priceDecimals()).toPlainString();
    }

    public BigDecimal toDecimal(long units) {
        return BigDecimal.valueOf(units, priceDecimals());
    }

    private static BigDecimal parseDecimal(String text, String field) {
        if (text == null || text.isBlank()) {
            throw new InvalidRequestException(field + " is required");
        }
        try {
            return new BigDecimal(text.trim());
        } catch (NumberFormatException e) {
            throw new InvalidRequestException(field + " is not a decimal number: " + text);
        }
    }
}
