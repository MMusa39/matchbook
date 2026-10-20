package dev.matchbook.gateway.risk;

/** Outcome of a pre-trade check: {@code null} reason means it passed. */
public record RiskCheck(String rejectReason, RiskManager.Reservation reservation) {

    static RiskCheck reject(String reason) {
        return new RiskCheck(reason, null);
    }

    public boolean passed() {
        return rejectReason == null;
    }
}
