package dev.matchbook.engine.core;

import dev.matchbook.engine.core.RecordingSink.Event;
import dev.matchbook.engine.core.RecordingSink.Exec;
import dev.matchbook.engine.core.RecordingSink.Level;
import dev.matchbook.engine.core.RecordingSink.Top;
import dev.matchbook.engine.core.RecordingSink.Trade;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static dev.matchbook.engine.core.EngineDriver.orderId;
import static dev.matchbook.engine.core.RecordingSink.execs;
import static dev.matchbook.engine.core.RecordingSink.levels;
import static dev.matchbook.engine.core.RecordingSink.tops;
import static dev.matchbook.engine.core.RecordingSink.trades;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

class MatchingEngineTest {

    private final EngineDriver d = new EngineDriver();

    @Nested
    class Resting {

        @Test
        void nonCrossingLimitOrderRestsAndPublishesDepthAndTopOfBook() {
            List<Event> events = d.buy(1, 100, 10);

            assertThat(events).hasSize(3);
            Exec ack = (Exec) events.get(0);
            assertThat(ack.type()).isEqualTo(ExecType.NEW);
            assertThat(ack.leaves()).isEqualTo(10);
            assertThat(ack.cum()).isZero();
            Level level = (Level) events.get(1);
            assertThat(level.side()).isEqualTo(Side.BUY);
            assertThat(level.price()).isEqualTo(100);
            assertThat(level.quantity()).isEqualTo(10);
            assertThat(level.orders()).isEqualTo(1);
            Top top = (Top) events.get(2);
            assertThat(top.bidPrice()).isEqualTo(100);
            assertThat(top.bidQuantity()).isEqualTo(10);
            assertThat(top.askPrice()).isZero();

            assertThat(d.book().bestBidPrice()).isEqualTo(100);
            assertThat(d.engine.restingOrderCount()).isEqualTo(1);
        }

        @Test
        void sequenceNumbersAreContiguousAcrossEventTypes() {
            d.buy(1, 100, 10);
            d.sell(2, 101, 10);
            List<Event> events = d.sell(3, 100, 4);

            assertThat(events).extracting(Event::seq)
                    .isEqualTo(List.of(7L, 8L, 9L, 10L, 11L, 12L));
            assertThat(d.engine.lastSeq()).isEqualTo(12);
        }

        @Test
        void perSymbolSequenceCountsOnlyMarketDataEvents() {
            d.buy(1, 100, 10);   // level + top -> symSeq 1,2
            List<Event> events = d.sell(2, 100, 10);  // trade, level, top -> 3,4,5

            assertThat(trades(events).getFirst().symbolSeq()).isEqualTo(3);
            assertThat(levels(events).getFirst().symbolSeq()).isEqualTo(4);
            assertThat(tops(events).getFirst().symbolSeq()).isEqualTo(5);
        }

        @Test
        void bestPricesTrackInsertsAndRemovals() {
            long b1 = orderId(d.buy(1, 98, 1));
            long b2 = orderId(d.buy(1, 99, 1));
            long a1 = orderId(d.sell(2, 103, 1));
            long a2 = orderId(d.sell(2, 102, 1));
            assertThat(d.book().bestBidPrice()).isEqualTo(99);
            assertThat(d.book().bestAskPrice()).isEqualTo(102);

            d.cancel(1, b2);
            d.cancel(2, a2);
            assertThat(d.book().bestBidPrice()).isEqualTo(98);
            assertThat(d.book().bestAskPrice()).isEqualTo(103);

            d.cancel(1, b1);
            d.cancel(2, a1);
            assertThat(d.book().bestBidPrice()).isZero();
            assertThat(d.book().bestAskPrice()).isZero();
            assertThat(d.book().restingOrders()).isZero();
        }

        @Test
        void ordersAtTheEdgesOfTheBandAreAccepted() {
            d.buy(1, 1, 1);
            d.sell(2, 10_000, 1);
            assertThat(d.book().bestBidPrice()).isEqualTo(1);
            assertThat(d.book().bestAskPrice()).isEqualTo(10_000);
        }
    }

    @Nested
    class Matching {

        @Test
        void fullFillAtRestingPriceGivesPriceImprovementToTheAggressor() {
            long sellId = orderId(d.sell(1, 100, 10));
            List<Event> events = d.buy(2, 105, 10);

            List<Trade> trades = trades(events);
            assertThat(trades).hasSize(1);
            Trade trade = trades.getFirst();
            assertThat(trade.price()).isEqualTo(100);
            assertThat(trade.quantity()).isEqualTo(10);
            assertThat(trade.aggressor()).isEqualTo(Side.BUY);
            assertThat(trade.sellOrderId()).isEqualTo(sellId);
            assertThat(trade.buyClientId()).isEqualTo(2);
            assertThat(trade.sellClientId()).isEqualTo(1);

            List<Exec> execs = execs(events);
            assertThat(execs).extracting(Exec::type)
                    .containsExactly(ExecType.NEW, ExecType.FILL, ExecType.FILL);
            assertThat(execs.get(1).orderId()).isEqualTo(sellId);
            assertThat(execs.get(1).liquidity()).isEqualTo(Liquidity.MAKER);
            assertThat(execs.get(2).liquidity()).isEqualTo(Liquidity.TAKER);
            assertThat(execs.get(2).lastPrice()).isEqualTo(100);
            assertThat(execs.get(2).tradeId()).isEqualTo(trade.tradeId());

            assertThat(d.book().restingOrders()).isZero();
            assertThat(d.engine.freeOrderCapacity()).isEqualTo(10_000);
        }

        @Test
        void eventOrderForAFillIsTradeThenMakerThenTaker() {
            d.sell(1, 100, 10);
            List<Event> events = d.buy(2, 100, 10);

            assertThat(events).extracting(e -> e.getClass().getSimpleName())
                    .containsExactly("Exec", "Trade", "Exec", "Exec", "Level", "Top");
        }

        @Test
        void partialFillOfTheIncomingOrderRestsTheRemainder() {
            d.sell(1, 100, 4);
            List<Event> events = d.buy(2, 100, 10);

            List<Exec> execs = execs(events);
            Exec taker = execs.get(2);
            assertThat(taker.type()).isEqualTo(ExecType.PARTIAL_FILL);
            assertThat(taker.leaves()).isEqualTo(6);
            assertThat(taker.cum()).isEqualTo(4);
            assertThat(d.book().bestBidPrice()).isEqualTo(100);
            assertThat(d.book().bestBidQuantity()).isEqualTo(6);
            assertThat(d.book().bestAskPrice()).isZero();
            assertThat(levels(events)).extracting(Level::side, Level::price, Level::quantity)
                    .containsExactly(
                            tuple(Side.SELL, 100L, 0L),
                            tuple(Side.BUY, 100L, 6L));
        }

        @Test
        void partialFillOfTheRestingOrderKeepsItAtTheFrontOfTheQueue() {
            long first = orderId(d.sell(1, 100, 10));
            long second = orderId(d.sell(2, 100, 10));
            d.buy(3, 100, 4);
            List<Event> events = d.buy(3, 100, 7);

            List<Trade> trades = trades(events);
            assertThat(trades).extracting(Trade::sellOrderId, Trade::quantity)
                    .containsExactly(
                            tuple(first, 6L),
                            tuple(second, 1L));
        }

        @Test
        void sweepsMultipleLevelsBestPriceFirstAndOneLevelUpdateEach() {
            d.sell(1, 102, 5);
            d.sell(1, 100, 5);
            d.sell(1, 101, 5);
            List<Event> events = d.buy(2, 102, 12);

            assertThat(trades(events)).extracting(Trade::price, Trade::quantity)
                    .containsExactly(
                            tuple(100L, 5L),
                            tuple(101L, 5L),
                            tuple(102L, 2L));
            assertThat(levels(events)).extracting(Level::price, Level::quantity)
                    .containsExactly(
                            tuple(100L, 0L),
                            tuple(101L, 0L),
                            tuple(102L, 3L));
            assertThat(tops(events)).singleElement()
                    .extracting(Top::askPrice, Top::askQuantity).containsExactly(102L, 3L);
        }

        @Test
        void timePriorityWithinALevel() {
            long a = orderId(d.buy(1, 100, 3));
            long b = orderId(d.buy(2, 100, 3));
            long c = orderId(d.buy(3, 100, 3));
            List<Event> events = d.sell(4, 100, 9);

            assertThat(trades(events)).extracting(Trade::buyOrderId).containsExactly(a, b, c);
        }

        @Test
        void limitPriceStopsTheSweep() {
            d.sell(1, 100, 5);
            d.sell(1, 103, 5);
            List<Event> events = d.buy(2, 101, 10);

            assertThat(trades(events)).extracting(Trade::price).containsExactly(100L);
            assertThat(d.book().bestBidPrice()).isEqualTo(101);
            assertThat(d.book().bestBidQuantity()).isEqualTo(5);
            assertThat(d.book().bestAskPrice()).isEqualTo(103);
            assertThat(d.book().isCrossed()).isFalse();
        }

        @Test
        void sellAggressorMatchesHighestBidFirst() {
            d.buy(1, 99, 5);
            d.buy(1, 101, 5);
            d.buy(1, 100, 5);
            List<Event> events = d.sell(2, 100, 8);

            assertThat(trades(events)).extracting(Trade::price, Trade::quantity)
                    .containsExactly(
                            tuple(101L, 5L),
                            tuple(100L, 3L));
            assertThat(trades(events).getFirst().aggressor()).isEqualTo(Side.SELL);
        }
    }

    @Nested
    class MarketIocFok {

        @Test
        void marketOrderSweepsAndCancelsUnfilledRemainder() {
            d.sell(1, 100, 5);
            d.sell(1, 500, 5);
            List<Event> events = d.order(2, Side.BUY, OrderType.MARKET, TimeInForce.IOC, StpMode.CANCEL_RESTING,
                    0, 15);

            assertThat(trades(events)).extracting(Trade::price).containsExactly(100L, 500L);
            Exec last = execs(events).getLast();
            assertThat(last.type()).isEqualTo(ExecType.CANCELED);
            assertThat(last.reason()).isEqualTo(Reason.IOC_REMAINDER);
            assertThat(last.cum()).isEqualTo(10);
            assertThat(last.leaves()).isZero();
            assertThat(d.book().restingOrders()).isZero();
        }

        @Test
        void marketOrderIntoEmptyBookIsCancelled() {
            List<Event> events = d.order(2, Side.SELL, OrderType.MARKET, TimeInForce.IOC, StpMode.CANCEL_RESTING,
                    0, 5);

            assertThat(execs(events)).extracting(Exec::type, Exec::reason).containsExactly(
                    tuple(ExecType.NEW, Reason.NONE),
                    tuple(ExecType.CANCELED, Reason.IOC_REMAINDER));
            assertThat(tops(events)).isEmpty();
        }

        @Test
        void marketOrderMustNotBeGtc() {
            List<Event> events = d.order(2, Side.BUY, OrderType.MARKET, TimeInForce.GTC, StpMode.CANCEL_RESTING,
                    0, 5);

            assertThat(execs(events)).singleElement().extracting(Exec::type, Exec::reason)
                    .containsExactly(ExecType.REJECTED, Reason.INVALID_TIME_IN_FORCE);
        }

        @Test
        void iocFillsWhatItCanAndNeverRests() {
            d.sell(1, 100, 4);
            List<Event> events = d.order(2, Side.BUY, OrderType.LIMIT, TimeInForce.IOC, StpMode.CANCEL_RESTING,
                    100, 10);

            assertThat(trades(events)).singleElement().extracting(Trade::quantity).isEqualTo(4L);
            assertThat(execs(events).getLast().reason()).isEqualTo(Reason.IOC_REMAINDER);
            assertThat(d.book().restingOrders()).isZero();
        }

        @Test
        void fokThatCannotFillCompletelyIsKilledWithNoSideEffects() {
            d.sell(1, 100, 4);
            d.sell(1, 101, 4);
            List<Event> events = d.order(2, Side.BUY, OrderType.LIMIT, TimeInForce.FOK, StpMode.CANCEL_RESTING,
                    101, 9);

            assertThat(trades(events)).isEmpty();
            assertThat(levels(events)).isEmpty();
            assertThat(execs(events)).extracting(Exec::type, Exec::reason).containsExactly(
                    tuple(ExecType.NEW, Reason.NONE),
                    tuple(ExecType.CANCELED, Reason.FOK_NOT_FILLED));
            assertThat(d.book().restingOrders()).isEqualTo(2);
        }

        @Test
        void fokThatCanFillCompletelyFillsAcrossLevels() {
            d.sell(1, 100, 4);
            d.sell(1, 101, 5);
            List<Event> events = d.order(2, Side.BUY, OrderType.LIMIT, TimeInForce.FOK, StpMode.CANCEL_RESTING,
                    101, 9);

            assertThat(trades(events)).extracting(Trade::quantity).containsExactly(4L, 5L);
            assertThat(execs(events).getLast().type()).isEqualTo(ExecType.FILL);
        }

        @Test
        void fokIgnoresLiquidityBeyondItsLimit() {
            d.sell(1, 100, 4);
            d.sell(1, 102, 100);
            List<Event> events = d.order(2, Side.BUY, OrderType.LIMIT, TimeInForce.FOK, StpMode.CANCEL_RESTING,
                    101, 5);

            assertThat(execs(events).getLast().reason()).isEqualTo(Reason.FOK_NOT_FILLED);
        }

        @Test
        void marketFokFillsOrKills() {
            d.sell(1, 100, 4);
            List<Event> killed = d.order(2, Side.BUY, OrderType.MARKET, TimeInForce.FOK, StpMode.CANCEL_RESTING,
                    0, 5);
            assertThat(execs(killed).getLast().reason()).isEqualTo(Reason.FOK_NOT_FILLED);

            List<Event> filled = d.order(2, Side.BUY, OrderType.MARKET, TimeInForce.FOK, StpMode.CANCEL_RESTING,
                    0, 4);
            assertThat(execs(filled).getLast().type()).isEqualTo(ExecType.FILL);
        }
    }

    @Nested
    class Cancel {

        @Test
        void cancelRemovesRestingOrderAndReportsBothClientIds() {
            List<Event> placed = d.buy(1, 100, 10);
            long id = orderId(placed);
            long originalClOrdId = execs(placed).getFirst().clOrdId();

            List<Event> events = d.cancel(1, id);

            Exec cancelled = execs(events).getFirst();
            assertThat(cancelled.type()).isEqualTo(ExecType.CANCELED);
            assertThat(cancelled.reason()).isEqualTo(Reason.CLIENT_CANCEL);
            assertThat(cancelled.origClOrdId()).isEqualTo(originalClOrdId);
            assertThat(cancelled.clOrdId()).isNotEqualTo(originalClOrdId);
            assertThat(cancelled.leaves()).isZero();
            assertThat(levels(events)).singleElement().extracting(Level::quantity).isEqualTo(0L);
            assertThat(tops(events)).singleElement().extracting(Top::bidPrice).isEqualTo(0L);
            assertThat(d.engine.restingOrder(id)).isNull();
            assertThat(d.engine.freeOrderCapacity()).isEqualTo(10_000);
        }

        @Test
        void cancelOfPartiallyFilledOrderReportsCumulativeQuantity() {
            long id = orderId(d.buy(1, 100, 10));
            d.sell(2, 100, 3);

            Exec cancelled = execs(d.cancel(1, id)).getFirst();
            assertThat(cancelled.cum()).isEqualTo(3);
            assertThat(cancelled.quantity()).isEqualTo(10);
        }

        @Test
        void cancelOfUnknownOrderIsRejected() {
            Exec reject = execs(d.cancel(1, 999)).getFirst();
            assertThat(reject.type()).isEqualTo(ExecType.CANCEL_REJECTED);
            assertThat(reject.reason()).isEqualTo(Reason.UNKNOWN_ORDER);
        }

        @Test
        void cancelOfAnotherClientsOrderLooksLikeAnUnknownOrder() {
            long id = orderId(d.buy(1, 100, 10));
            Exec reject = execs(d.cancel(2, id)).getFirst();
            assertThat(reject.type()).isEqualTo(ExecType.CANCEL_REJECTED);
            assertThat(reject.reason()).isEqualTo(Reason.UNKNOWN_ORDER);
            assertThat(d.engine.restingOrder(id)).isNotNull();
        }

        @Test
        void cancelOfFilledOrderIsRejected() {
            long id = orderId(d.buy(1, 100, 10));
            d.sell(2, 100, 10);
            assertThat(execs(d.cancel(1, id)).getFirst().type()).isEqualTo(ExecType.CANCEL_REJECTED);
        }

        @Test
        void cancelInTheMiddleOfAQueueKeepsTheRestInOrder() {
            long a = orderId(d.buy(1, 100, 1));
            long b = orderId(d.buy(1, 100, 1));
            long c = orderId(d.buy(1, 100, 1));
            d.cancel(1, b);

            List<Event> events = d.sell(2, 100, 2);
            assertThat(trades(events)).extracting(Trade::buyOrderId).containsExactly(a, c);
        }
    }

    @Nested
    class Replace {

        @Test
        void quantityDecreaseKeepsQueuePriority() {
            long first = orderId(d.buy(1, 100, 10));
            long second = orderId(d.buy(2, 100, 10));
            List<Event> replaced = d.replace(1, first, 100, 4);

            Exec ack = execs(replaced).getFirst();
            assertThat(ack.type()).isEqualTo(ExecType.REPLACED);
            assertThat(ack.leaves()).isEqualTo(4);
            assertThat(levels(replaced)).singleElement().extracting(Level::quantity).isEqualTo(14L);

            List<Event> events = d.sell(3, 100, 5);
            assertThat(trades(events)).extracting(Trade::buyOrderId, Trade::quantity).containsExactly(
                    tuple(first, 4L),
                    tuple(second, 1L));
        }

        @Test
        void quantityIncreaseLosesQueuePriority() {
            long first = orderId(d.buy(1, 100, 10));
            long second = orderId(d.buy(2, 100, 10));
            d.replace(1, first, 100, 11);

            List<Event> events = d.sell(3, 100, 5);
            assertThat(trades(events)).extracting(Trade::buyOrderId).containsExactly(second);
        }

        @Test
        void priceChangeLosesQueuePriorityEvenWhenMovedBack() {
            long first = orderId(d.buy(1, 100, 10));
            long second = orderId(d.buy(2, 100, 10));
            d.replace(1, first, 99, 10);
            d.replace(1, first, 100, 10);

            List<Event> events = d.sell(3, 100, 15);
            assertThat(trades(events)).extracting(Trade::buyOrderId, Trade::quantity).containsExactly(
                    tuple(second, 10L),
                    tuple(first, 5L));
        }

        @Test
        void replaceThatCrossesTradesImmediately() {
            d.sell(2, 101, 5);
            long id = orderId(d.buy(1, 100, 10));
            List<Event> events = d.replace(1, id, 101, 10);

            assertThat(execs(events).getFirst().type()).isEqualTo(ExecType.REPLACED);
            assertThat(trades(events)).singleElement().extracting(Trade::price, Trade::quantity)
                    .containsExactly(101L, 5L);
            assertThat(d.book().bestBidPrice()).isEqualTo(101);
            assertThat(d.book().bestBidQuantity()).isEqualTo(5);
            assertThat(d.engine.restingOrder(id).orderId()).isEqualTo(id);
        }

        @Test
        void replaceKeepsFilledQuantityUsingFixOrderQtySemantics() {
            long id = orderId(d.buy(1, 100, 10));
            d.sell(2, 100, 3);
            Exec ack = execs(d.replace(1, id, 100, 5)).getFirst();

            assertThat(ack.quantity()).isEqualTo(5);
            assertThat(ack.cum()).isEqualTo(3);
            assertThat(ack.leaves()).isEqualTo(2);
        }

        @Test
        void replaceToQuantityAtOrBelowFilledIsRejected() {
            long id = orderId(d.buy(1, 100, 10));
            d.sell(2, 100, 3);
            Exec reject = execs(d.replace(1, id, 100, 3)).getFirst();

            assertThat(reject.type()).isEqualTo(ExecType.REPLACE_REJECTED);
            assertThat(reject.reason()).isEqualTo(Reason.INVALID_QUANTITY);
            assertThat(d.engine.restingOrder(id).leavesQuantity()).isEqualTo(7);
        }

        @Test
        void invalidReplaceLeavesOrderUntouched() {
            long id = orderId(d.buy(1, 100, 10));
            Exec reject = execs(d.replace(1, id, 20_000, 10)).getFirst();

            assertThat(reject.type()).isEqualTo(ExecType.REPLACE_REJECTED);
            assertThat(reject.reason()).isEqualTo(Reason.PRICE_OUT_OF_BAND);
            assertThat(d.engine.restingOrder(id).price()).isEqualTo(100);
        }

        @Test
        void replaceOfUnknownOrderIsRejected() {
            Exec reject = execs(d.replace(1, 12345, 100, 10)).getFirst();
            assertThat(reject.type()).isEqualTo(ExecType.REPLACE_REJECTED);
            assertThat(reject.reason()).isEqualTo(Reason.UNKNOWN_ORDER);
        }

        @Test
        void replaceUpdatesClOrdIdAndReportsTheOriginal() {
            List<Event> placed = d.buy(1, 100, 10);
            long id = orderId(placed);
            long original = execs(placed).getFirst().clOrdId();
            Exec ack = execs(d.replace(1, id, 100, 9)).getFirst();

            assertThat(ack.origClOrdId()).isEqualTo(original);
            assertThat(d.engine.restingOrder(id).clOrdId()).isEqualTo(ack.clOrdId());
        }
    }

    @Nested
    class SelfTradePrevention {

        @Test
        void cancelRestingCancelsOwnOrderAndKeepsMatching() {
            long own = orderId(d.sell(1, 100, 5));
            long other = orderId(d.sell(2, 100, 5));
            List<Event> events = d.order(1, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, StpMode.CANCEL_RESTING,
                    100, 5);

            List<Exec> execs = execs(events);
            assertThat(execs.get(1).orderId()).isEqualTo(own);
            assertThat(execs.get(1).type()).isEqualTo(ExecType.CANCELED);
            assertThat(execs.get(1).reason()).isEqualTo(Reason.SELF_TRADE_PREVENTION);
            assertThat(trades(events)).singleElement().extracting(Trade::sellOrderId).isEqualTo(other);
            assertThat(trades(events)).allMatch(t -> t.buyClientId() != t.sellClientId());
        }

        @Test
        void cancelAggressorStopsAtOwnOrder() {
            long other = orderId(d.sell(2, 100, 3));
            long own = orderId(d.sell(1, 100, 5));
            d.sell(2, 100, 5);
            List<Event> events = d.order(1, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, StpMode.CANCEL_AGGRESSOR,
                    100, 10);

            assertThat(trades(events)).singleElement().extracting(Trade::sellOrderId).isEqualTo(other);
            Exec last = execs(events).getLast();
            assertThat(last.type()).isEqualTo(ExecType.CANCELED);
            assertThat(last.reason()).isEqualTo(Reason.SELF_TRADE_PREVENTION);
            assertThat(last.cum()).isEqualTo(3);
            assertThat(d.engine.restingOrder(own)).isNotNull();
            assertThat(d.book().bestBidPrice()).isZero();
        }

        @Test
        void cancelBothCancelsOwnRestingOrderAndTheAggressor() {
            long own = orderId(d.sell(1, 100, 5));
            d.sell(2, 100, 5);
            List<Event> events = d.order(1, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, StpMode.CANCEL_BOTH,
                    100, 10);

            assertThat(trades(events)).isEmpty();
            assertThat(execs(events)).extracting(Exec::orderId, Exec::type).containsExactly(
                    tuple(own + 2, ExecType.NEW),
                    tuple(own, ExecType.CANCELED),
                    tuple(own + 2, ExecType.CANCELED));
            assertThat(d.engine.restingOrder(own)).isNull();
            assertThat(d.book().bestAskQuantity()).isEqualTo(5);
        }

        @Test
        void cancelAggressorDoesNotEmitLevelUpdateForUntouchedLevel() {
            d.sell(1, 100, 5);
            List<Event> events = d.order(1, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, StpMode.CANCEL_AGGRESSOR,
                    100, 10);

            assertThat(levels(events)).isEmpty();
            assertThat(tops(events)).isEmpty();
        }

        @Test
        void fokCountsOnlyLiquidityThatSelfTradePreventionWouldLetItReach() {
            d.sell(1, 100, 5);   // own
            d.sell(2, 100, 5);
            // CANCEL_RESTING skips the own order: only 5 available from others.
            List<Event> killed = d.order(1, Side.BUY, OrderType.LIMIT, TimeInForce.FOK, StpMode.CANCEL_RESTING,
                    100, 6);
            assertThat(execs(killed).getLast().reason()).isEqualTo(Reason.FOK_NOT_FILLED);
            // CANCEL_AGGRESSOR stops at the own order, which is first in the queue: nothing reachable.
            List<Event> killed2 = d.order(1, Side.BUY, OrderType.LIMIT, TimeInForce.FOK, StpMode.CANCEL_AGGRESSOR,
                    100, 1);
            assertThat(execs(killed2).getLast().reason()).isEqualTo(Reason.FOK_NOT_FILLED);
            assertThat(d.book().restingOrders()).isEqualTo(2);

            List<Event> filled = d.order(1, Side.BUY, OrderType.LIMIT, TimeInForce.FOK, StpMode.CANCEL_RESTING,
                    100, 5);
            assertThat(execs(filled).getLast().type()).isEqualTo(ExecType.FILL);
        }
    }

    @Nested
    class Validation {

        private final EngineDriver strict = new EngineDriver(new EngineConfig(0, 100, List.of(
                new SymbolConfig(EngineDriver.SYMBOL, "STRICT", 100, 5, 10, 10, 1_000, 50, 200))));

        @ParameterizedTest(name = "{0} px={1} qty={2} -> {3}")
        @CsvSource({
                "LIMIT, 100, 0,    INVALID_QUANTITY",
                "LIMIT, 100, -10,  INVALID_QUANTITY",
                "LIMIT, 100, 1010, INVALID_QUANTITY",
                "LIMIT, 100, 15,   INVALID_LOT_SIZE",
                "LIMIT, 0,   10,   INVALID_PRICE",
                "LIMIT, -5,  10,   INVALID_PRICE",
                "LIMIT, 101, 10,   INVALID_TICK_SIZE",
                "LIMIT, 45,  10,   PRICE_OUT_OF_BAND",
                "LIMIT, 205, 10,   PRICE_OUT_OF_BAND",
                "LIMIT, 50,  10,   NONE",
                "LIMIT, 200, 1000, NONE",
                "MARKET, 0,  15,   INVALID_LOT_SIZE",
        })
        void rejectsInvalidOrders(OrderType type, long price, long quantity, Reason expected) {
            TimeInForce tif = type == OrderType.MARKET ? TimeInForce.IOC : TimeInForce.GTC;
            List<Event> events = strict.order(1, Side.BUY, type, tif, StpMode.CANCEL_RESTING, price, quantity);

            Exec first = execs(events).getFirst();
            if (expected == Reason.NONE) {
                assertThat(first.type()).isEqualTo(ExecType.NEW);
            } else {
                assertThat(first.type()).isEqualTo(ExecType.REJECTED);
                assertThat(first.reason()).isEqualTo(expected);
                assertThat(events).hasSize(1);
                assertThat(first.orderId()).isZero();
            }
        }

        @Test
        void unknownSymbolIsRejected() {
            List<Event> events = d.process(new Command().newOrder(1, 1, 1, 42, Side.BUY, OrderType.LIMIT,
                    TimeInForce.GTC, StpMode.CANCEL_RESTING, 100, 10));
            assertThat(execs(events).getFirst().reason()).isEqualTo(Reason.UNKNOWN_SYMBOL);
        }

        @Test
        void missingEnumFieldIsRejected() {
            List<Event> events = d.process(new Command().newOrder(1, 1, 1, EngineDriver.SYMBOL, null,
                    OrderType.LIMIT, TimeInForce.GTC, StpMode.CANCEL_RESTING, 100, 10));
            assertThat(execs(events).getFirst().reason()).isEqualTo(Reason.INVALID_FIELD);
        }

        @Test
        void rejectedOrdersDoNotConsumeOrderIds() {
            strict.order(1, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, StpMode.CANCEL_RESTING, 101, 10);
            long id = orderId(strict.order(1, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, StpMode.CANCEL_RESTING,
                    100, 10));
            assertThat(id).isEqualTo(1);
        }
    }

    @Nested
    class Capacity {

        @Test
        void rejectsWhenThePoolIsExhaustedAndRecoversWhenOrdersLeave() {
            EngineDriver small = new EngineDriver(new EngineConfig(0, 2, List.of(EngineDriver.testSymbol(1))));
            long first = orderId(small.buy(1, 100, 1));
            small.buy(1, 101, 1);

            Exec rejected = execs(small.buy(1, 102, 1)).getFirst();
            assertThat(rejected.type()).isEqualTo(ExecType.REJECTED);
            assertThat(rejected.reason()).isEqualTo(Reason.ORDER_CAPACITY_EXCEEDED);

            small.cancel(1, first);
            assertThat(execs(small.buy(1, 102, 1)).getFirst().type()).isEqualTo(ExecType.NEW);
        }
    }

    @Nested
    class Sharding {

        @Test
        void shardIdIsEncodedInOrderAndTradeIds() {
            EngineDriver shard3 = new EngineDriver(new EngineConfig(3, 10, List.of(EngineDriver.testSymbol(1))));
            long sellId = orderId(shard3.sell(1, 100, 1));
            Trade trade = trades(shard3.buy(2, 100, 1)).getFirst();

            assertThat(sellId).isEqualTo((3L << 48) + 1);
            assertThat(trade.tradeId()).isEqualTo((3L << 48) + 1);
            assertThat(trade.buyOrderId()).isEqualTo((3L << 48) + 2);
        }

        @Test
        void symbolsAreIndependentBooks() {
            EngineDriver multi = new EngineDriver(new EngineConfig(0, 10,
                    List.of(EngineDriver.testSymbol(1), EngineDriver.testSymbol(2))));
            multi.process(new Command().newOrder(1, 1, 1, 1, Side.SELL, OrderType.LIMIT, TimeInForce.GTC,
                    StpMode.CANCEL_RESTING, 100, 5));
            List<Event> events = multi.process(new Command().newOrder(2, 2, 2, 2, Side.BUY, OrderType.LIMIT,
                    TimeInForce.GTC, StpMode.CANCEL_RESTING, 100, 5));

            assertThat(trades(events)).isEmpty();
            assertThat(multi.engine.orderBook(1).bestAskPrice()).isEqualTo(100);
            assertThat(multi.engine.orderBook(2).bestBidPrice()).isEqualTo(100);
            assertThat(multi.engine.orderBook(2).symbolSeq()).isEqualTo(2);
        }
    }

    @Nested
    class TopOfBookEmission {

        @Test
        void notEmittedWhenADeeperLevelChanges() {
            d.buy(1, 100, 10);
            List<Event> events = d.buy(1, 99, 10);
            assertThat(tops(events)).isEmpty();
        }

        @Test
        void emittedWhenBestSizeChanges() {
            d.buy(1, 100, 10);
            List<Event> events = d.buy(1, 100, 5);
            assertThat(tops(events)).singleElement().extracting(Top::bidQuantity).isEqualTo(15L);
        }
    }
}
