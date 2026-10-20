package dev.matchbook.gateway.order;

import dev.matchbook.gateway.admission.RedisAdmission;
import dev.matchbook.gateway.reference.Symbol;
import dev.matchbook.gateway.reference.SymbolRegistry;
import dev.matchbook.proto.v1.ClientExecutionReport;
import dev.matchbook.proto.v1.ExecutionReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Routes engine execution reports to the client sessions that are streaming them, translating fixed-point
 * values back to decimals and internal client order ids back to the client's own ids.
 */
@Component
public class ExecutionRouter {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionRouter.class);
    private static final int ID_CACHE_LIMIT = 1_000_000;

    private final SymbolRegistry symbols;
    private final RedisAdmission admission;
    private final Map<Long, Set<Consumer<ClientExecutionReport>>> sessions = new ConcurrentHashMap<>();
    private record Key(long clientId, long internalClOrdId) {
    }

    private final Map<Key, String> externalIds = new ConcurrentHashMap<>();

    public ExecutionRouter(SymbolRegistry symbols, RedisAdmission admission) {
        this.symbols = symbols;
        this.admission = admission;
    }

    public Runnable subscribe(long clientId, Consumer<ClientExecutionReport> session) {
        sessions.computeIfAbsent(clientId, c -> ConcurrentHashMap.newKeySet()).add(session);
        return () -> sessions.getOrDefault(clientId, Set.of()).remove(session);
    }

    /** Remembers the external id of an internal one, so routing usually needs no Redis lookup. */
    void remember(long clientId, long internalClOrdId, String externalClOrdId) {
        if (externalIds.size() > ID_CACHE_LIMIT) {
            externalIds.clear();
        }
        externalIds.put(new Key(clientId, internalClOrdId), externalClOrdId);
    }

    public void route(ExecutionReport report) {
        Set<Consumer<ClientExecutionReport>> targets = sessions.get(report.getClientId());
        if (targets == null || targets.isEmpty()) {
            return;
        }
        Symbol symbol = symbols.byId(report.getSymbolId());
        if (symbol == null) {
            return;
        }
        ClientExecutionReport message = ClientExecutionReport.newBuilder()
                .setSeq(report.getSeq())
                .setClientOrderId(externalId(report.getClientId(), report.getClOrdId()))
                .setOrderId(report.getOrderId())
                .setSymbol(symbol.symbol())
                .setExecType(report.getExecType())
                .setReason(report.getReason())
                .setSide(report.getSide())
                .setPrice(symbol.formatPrice(report.getPrice()))
                .setQuantity(Long.toString(report.getQuantity()))
                .setLastPrice(symbol.formatPrice(report.getLastPrice()))
                .setLastQuantity(Long.toString(report.getLastQuantity()))
                .setLeavesQuantity(Long.toString(report.getLeavesQuantity()))
                .setCumQuantity(Long.toString(report.getCumQuantity()))
                .setTradeId(report.getTradeId())
                .setTimestamp(report.getTimestamp())
                .build();
        for (Consumer<ClientExecutionReport> session : targets) {
            try {
                session.accept(message);
            } catch (RuntimeException e) {
                LOG.debug("dropping failed execution stream for client {}", report.getClientId(), e);
                targets.remove(session);
            }
        }
    }

    String externalId(long clientId, long internalClOrdId) {
        String cached = externalIds.get(new Key(clientId, internalClOrdId));
        if (cached != null) {
            return cached;
        }
        String fromRedis = admission.externalClOrdId(clientId, internalClOrdId);
        return fromRedis == null ? Long.toString(internalClOrdId) : fromRedis;
    }
}
