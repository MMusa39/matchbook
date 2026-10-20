package dev.matchbook.posttrade;

import dev.matchbook.proto.v1.ExecutionReport;
import dev.matchbook.proto.v1.Side;
import dev.matchbook.proto.v1.Trade;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies engine events to PostgreSQL, exactly once per event.
 *
 * <p>Each method runs in one transaction that (1) claims the event's {@code (shard, seq)} in
 * {@code processed_events}, stopping if it was already claimed, (2) writes the state change and (3) writes
 * the resulting events to the outbox. Kafka offsets are committed only after this transaction commits, so
 * a crash at any point leads to redelivery, and redelivery is harmless.
 */
@Service
public class TradeProcessor {

    private final JdbcTemplate jdbc;
    private final JsonMapper json;
    private final String positionsTopic;
    private final Counter applied;
    private final Counter duplicates;

    public TradeProcessor(JdbcTemplate jdbc, JsonMapper json, MeterRegistry meters,
                          @Value("${matchbook.positions-topic:matchbook.positions}") String positionsTopic) {
        this.jdbc = jdbc;
        this.json = json;
        this.positionsTopic = positionsTopic;
        this.applied = Counter.builder("matchbook.posttrade.events").tag("result", "applied").register(meters);
        this.duplicates = Counter.builder("matchbook.posttrade.events").tag("result", "duplicate").register(meters);
    }

    /** An engine event with its de-duplication key. */
    public record Event<T>(T payload, int shard, long seq) {
    }

    /** Applies a batch of trades in one transaction, in order; duplicates are skipped. Returns how many were new. */
    @Transactional
    public int applyTrades(List<Event<Trade>> events) {
        int applied = 0;
        for (Event<Trade> e : events) {
            if (applyTrade(e.payload(), e.shard(), e.seq())) {
                applied++;
            }
        }
        return applied;
    }

    /** Applies a batch of execution reports in one transaction; duplicates are skipped. Returns how many were new. */
    @Transactional
    public int applyExecutions(List<Event<ExecutionReport>> events) {
        int applied = 0;
        for (Event<ExecutionReport> e : events) {
            if (applyExecution(e.payload(), e.shard(), e.seq())) {
                applied++;
            }
        }
        return applied;
    }

    /** @return false if the event had already been applied */
    @Transactional
    public boolean applyTrade(Trade trade, int shard, long seq) {
        if (!claim(shard, seq, "trades")) {
            duplicates.increment();
            return false;
        }
        jdbc.update("""
                insert into trades (trade_id, shard, seq, symbol_id, price, quantity, aggressor_side,
                                    buy_order_id, sell_order_id, buy_client_id, sell_client_id, engine_ts)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                trade.getTradeId(), shard, seq, trade.getSymbolId(), trade.getPrice(), trade.getQuantity(),
                trade.getAggressorSide() == Side.SIDE_BUY ? "BUY" : "SELL", trade.getBuyOrderId(),
                trade.getSellOrderId(), trade.getBuyClientId(), trade.getSellClientId(), trade.getTimestamp());
        // Lock both rows in a fixed order (by client id) so concurrent trades between the same two clients
        // cannot deadlock.
        long first = Math.min(trade.getBuyClientId(), trade.getSellClientId());
        long second = Math.max(trade.getBuyClientId(), trade.getSellClientId());
        for (long client : first == second ? List.of(first) : List.of(first, second)) {
            if (client == trade.getBuyClientId()) {
                updatePosition(client, trade, true);
            }
            if (client == trade.getSellClientId()) {
                updatePosition(client, trade, false);
            }
        }
        applied.increment();
        return true;
    }

    /** Stores an execution report in the drop-copy table. */
    @Transactional
    public boolean applyExecution(ExecutionReport r, int shard, long seq) {
        if (!claim(shard, seq, "executions")) {
            duplicates.increment();
            return false;
        }
        jdbc.update("""
                insert into executions (shard, seq, exec_type, reason, order_id, client_id, cl_ord_id, symbol_id, side,
                                        price, quantity, last_price, last_qty, leaves_qty, cum_qty, trade_id, engine_ts)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                shard, seq, r.getExecType().name().replace("EXEC_TYPE_", ""), r.getReason(), r.getOrderId(),
                r.getClientId(), r.getClOrdId(), r.getSymbolId(), r.getSide().name().replace("SIDE_", ""),
                r.getPrice(), r.getQuantity(), r.getLastPrice(), r.getLastQuantity(), r.getLeavesQuantity(),
                r.getCumQuantity(), r.getTradeId(), r.getTimestamp());
        applied.increment();
        return true;
    }

    private boolean claim(int shard, long seq, String topic) {
        return jdbc.update("insert into processed_events (shard, seq, topic) values (?, ?, ?) on conflict do nothing",
                shard, seq, topic) == 1;
    }

    private void updatePosition(long clientId, Trade trade, boolean buy) {
        jdbc.update("insert into positions (client_id, symbol_id) values (?, ?) on conflict do nothing",
                clientId, trade.getSymbolId());
        Map<String, Object> row = jdbc.queryForMap("""
                select quantity, avg_price, realized_pnl, bought, sold, version
                from positions where client_id = ? and symbol_id = ? for update""", clientId, trade.getSymbolId());
        Position before = new Position(((Number) row.get("quantity")).longValue(), (BigDecimal) row.get("avg_price"),
                (BigDecimal) row.get("realized_pnl"), ((Number) row.get("bought")).longValue(),
                ((Number) row.get("sold")).longValue());
        Position after = before.apply(buy, trade.getQuantity(), trade.getPrice());
        long version = ((Number) row.get("version")).longValue() + 1;
        jdbc.update("""
                update positions set quantity = ?, avg_price = ?, realized_pnl = ?, bought = ?, sold = ?, version = ?,
                                     last_trade_id = ?, updated_at = now()
                where client_id = ? and symbol_id = ?""",
                after.quantity(), after.avgPrice(), after.realizedPnl(), after.bought(), after.sold(), version,
                trade.getTradeId(), clientId, trade.getSymbolId());

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("clientId", clientId);
        event.put("symbolId", trade.getSymbolId());
        event.put("version", version);
        event.put("tradeId", trade.getTradeId());
        event.put("quantity", after.quantity());
        event.put("avgPrice", after.avgPrice().toPlainString());
        event.put("realizedPnl", after.realizedPnl().toPlainString());
        jdbc.update("insert into outbox (topic, msg_key, event_type, payload) values (?, ?, 'PositionChanged', ?::jsonb)",
                positionsTopic, Long.toString(clientId), json.writeValueAsString(event));
    }
}
