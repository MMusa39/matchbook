package dev.matchbook.engine.server;

import dev.matchbook.engine.core.Command;
import dev.matchbook.engine.core.ExecType;
import dev.matchbook.engine.core.Liquidity;
import dev.matchbook.engine.core.OrderType;
import dev.matchbook.engine.core.Side;
import dev.matchbook.engine.core.StpMode;
import dev.matchbook.engine.core.TimeInForce;
import dev.matchbook.engine.pipeline.OutboundEvent;
import dev.matchbook.proto.v1.EngineCommand;
import dev.matchbook.proto.v1.ExecutionReport;
import dev.matchbook.proto.v1.LevelUpdate;
import dev.matchbook.proto.v1.TopOfBook;
import dev.matchbook.proto.v1.Trade;

/** Conversions between engine types and Protobuf messages. Runs on gRPC and listener threads, never on the matching thread. */
final class ProtoMapping {

    private ProtoMapping() {
    }

    /** Fills {@code target} from a gRPC command. Unknown enum values become {@code null}, which the engine rejects. */
    static Command toCommand(EngineCommand request, long timestamp, Command target) {
        return switch (request.getCommandCase()) {
            case NEW_ORDER -> {
                var n = request.getNewOrder();
                yield target.newOrder(timestamp, n.getClientId(), n.getClOrdId(), n.getSymbolId(), side(n.getSide()),
                        orderType(n.getOrderType()), timeInForce(n.getTimeInForce()), stpMode(n.getStpMode()),
                        n.getPrice(), n.getQuantity());
            }
            case CANCEL -> {
                var c = request.getCancel();
                yield target.cancel(timestamp, c.getClientId(), c.getClOrdId(), c.getSymbolId(), c.getOrderId());
            }
            case REPLACE -> {
                var r = request.getReplace();
                yield target.replace(timestamp, r.getClientId(), r.getClOrdId(), r.getSymbolId(), r.getOrderId(),
                        r.getPrice(), r.getQuantity());
            }
            case COMMAND_NOT_SET -> throw new IllegalArgumentException("command not set");
        };
    }

    static int symbolOf(EngineCommand request) {
        return switch (request.getCommandCase()) {
            case NEW_ORDER -> request.getNewOrder().getSymbolId();
            case CANCEL -> request.getCancel().getSymbolId();
            case REPLACE -> request.getReplace().getSymbolId();
            case COMMAND_NOT_SET -> -1;
        };
    }

    static ExecutionReport executionReport(OutboundEvent e, int shard) {
        return ExecutionReport.newBuilder()
                .setSeq(e.seq())
                .setShard(shard)
                .setTimestamp(e.timestamp())
                .setExecType(execType(e.execType()))
                .setReason(e.reason().name())
                .setOrderId(e.orderId())
                .setClientId(e.clientId())
                .setClOrdId(e.clOrdId())
                .setOrigClOrdId(e.origClOrdId())
                .setSymbolId(e.symbolId())
                .setSide(side(e.side()))
                .setOrderType(orderType(e.orderType()))
                .setTimeInForce(timeInForce(e.timeInForce()))
                .setPrice(e.price())
                .setQuantity(e.quantity())
                .setLastPrice(e.lastPrice())
                .setLastQuantity(e.lastQuantity())
                .setLeavesQuantity(e.leavesQuantity())
                .setCumQuantity(e.cumQuantity())
                .setTradeId(e.tradeId())
                .setLiquidity(liquidity(e.liquidity()))
                .build();
    }

    static Trade trade(OutboundEvent e, int shard) {
        return Trade.newBuilder()
                .setSeq(e.seq())
                .setShard(shard)
                .setSymbolSeq(e.symbolSeq())
                .setTimestamp(e.timestamp())
                .setTradeId(e.tradeId())
                .setSymbolId(e.symbolId())
                .setPrice(e.price())
                .setQuantity(e.quantity())
                .setAggressorSide(side(e.aggressorSide()))
                .setBuyOrderId(e.buyOrderId())
                .setSellOrderId(e.sellOrderId())
                .setBuyClientId(e.buyClientId())
                .setSellClientId(e.sellClientId())
                .build();
    }

    static LevelUpdate level(OutboundEvent e) {
        return LevelUpdate.newBuilder()
                .setSymbolId(e.symbolId())
                .setSymbolSeq(e.symbolSeq())
                .setSide(side(e.side()))
                .setPrice(e.price())
                .setQuantity(e.quantity())
                .setOrderCount(e.orderCount())
                .setTimestamp(e.timestamp())
                .build();
    }

    static TopOfBook top(OutboundEvent e) {
        return TopOfBook.newBuilder()
                .setSymbolId(e.symbolId())
                .setSymbolSeq(e.symbolSeq())
                .setBidPrice(e.bidPrice())
                .setBidQuantity(e.bidQuantity())
                .setAskPrice(e.askPrice())
                .setAskQuantity(e.askQuantity())
                .setTimestamp(e.timestamp())
                .build();
    }

    static Side side(dev.matchbook.proto.v1.Side side) {
        return switch (side) {
            case SIDE_BUY -> Side.BUY;
            case SIDE_SELL -> Side.SELL;
            default -> null;
        };
    }

    static dev.matchbook.proto.v1.Side side(Side side) {
        if (side == null) {
            return dev.matchbook.proto.v1.Side.SIDE_UNSPECIFIED;
        }
        return side == Side.BUY ? dev.matchbook.proto.v1.Side.SIDE_BUY : dev.matchbook.proto.v1.Side.SIDE_SELL;
    }

    static OrderType orderType(dev.matchbook.proto.v1.OrderType type) {
        return switch (type) {
            case ORDER_TYPE_LIMIT -> OrderType.LIMIT;
            case ORDER_TYPE_MARKET -> OrderType.MARKET;
            default -> null;
        };
    }

    static dev.matchbook.proto.v1.OrderType orderType(OrderType type) {
        if (type == null) {
            return dev.matchbook.proto.v1.OrderType.ORDER_TYPE_UNSPECIFIED;
        }
        return type == OrderType.LIMIT ? dev.matchbook.proto.v1.OrderType.ORDER_TYPE_LIMIT
                : dev.matchbook.proto.v1.OrderType.ORDER_TYPE_MARKET;
    }

    static TimeInForce timeInForce(dev.matchbook.proto.v1.TimeInForce tif) {
        return switch (tif) {
            case TIME_IN_FORCE_GTC -> TimeInForce.GTC;
            case TIME_IN_FORCE_IOC -> TimeInForce.IOC;
            case TIME_IN_FORCE_FOK -> TimeInForce.FOK;
            default -> null;
        };
    }

    static dev.matchbook.proto.v1.TimeInForce timeInForce(TimeInForce tif) {
        if (tif == null) {
            return dev.matchbook.proto.v1.TimeInForce.TIME_IN_FORCE_UNSPECIFIED;
        }
        return switch (tif) {
            case GTC -> dev.matchbook.proto.v1.TimeInForce.TIME_IN_FORCE_GTC;
            case IOC -> dev.matchbook.proto.v1.TimeInForce.TIME_IN_FORCE_IOC;
            case FOK -> dev.matchbook.proto.v1.TimeInForce.TIME_IN_FORCE_FOK;
        };
    }

    static StpMode stpMode(dev.matchbook.proto.v1.StpMode mode) {
        return switch (mode) {
            case STP_MODE_CANCEL_AGGRESSOR -> StpMode.CANCEL_AGGRESSOR;
            case STP_MODE_CANCEL_BOTH -> StpMode.CANCEL_BOTH;
            default -> StpMode.CANCEL_RESTING;
        };
    }

    static dev.matchbook.proto.v1.ExecType execType(ExecType type) {
        return switch (type) {
            case NEW -> dev.matchbook.proto.v1.ExecType.EXEC_TYPE_NEW;
            case PARTIAL_FILL -> dev.matchbook.proto.v1.ExecType.EXEC_TYPE_PARTIAL_FILL;
            case FILL -> dev.matchbook.proto.v1.ExecType.EXEC_TYPE_FILL;
            case CANCELED -> dev.matchbook.proto.v1.ExecType.EXEC_TYPE_CANCELED;
            case REPLACED -> dev.matchbook.proto.v1.ExecType.EXEC_TYPE_REPLACED;
            case REJECTED -> dev.matchbook.proto.v1.ExecType.EXEC_TYPE_REJECTED;
            case CANCEL_REJECTED -> dev.matchbook.proto.v1.ExecType.EXEC_TYPE_CANCEL_REJECTED;
            case REPLACE_REJECTED -> dev.matchbook.proto.v1.ExecType.EXEC_TYPE_REPLACE_REJECTED;
        };
    }

    static dev.matchbook.proto.v1.Liquidity liquidity(Liquidity liquidity) {
        return switch (liquidity) {
            case MAKER -> dev.matchbook.proto.v1.Liquidity.LIQUIDITY_MAKER;
            case TAKER -> dev.matchbook.proto.v1.Liquidity.LIQUIDITY_TAKER;
            case NONE -> dev.matchbook.proto.v1.Liquidity.LIQUIDITY_UNSPECIFIED;
        };
    }
}
