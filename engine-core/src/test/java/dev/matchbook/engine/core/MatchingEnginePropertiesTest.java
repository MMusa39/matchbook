package dev.matchbook.engine.core;

import dev.matchbook.engine.core.RecordingSink.Event;
import dev.matchbook.engine.core.RecordingSink.Exec;
import dev.matchbook.engine.core.RecordingSink.Trade;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;
import net.jqwik.api.statistics.Statistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests over random command sequences. Every command's events are checked against a
 * shadow model of the book that is rebuilt from the events alone, so the checks do not trust the
 * engine's own data structures.
 */
class MatchingEnginePropertiesTest {

    /** Abstract command; ids for cancel/replace are resolved against orders created so far. */
    record Spec(CommandType kind, int client, Side side, OrderType type, TimeInForce tif, StpMode stp,
                long price, long quantity, int target, boolean asOwner) {
    }

    @Provide
    Arbitrary<List<Spec>> commands() {
        return commands(6, 2, 2, 95, 105, 70).list().ofMinSize(1).ofMaxSize(300);
    }

    /**
     * Mostly resting orders over a wide price range, so the book is deep and the occupancy bitmap is
     * scanned across several 64-level words.
     */
    @Provide
    Arbitrary<List<Spec>> deepBooks() {
        // Bids mostly in 1..230 and offers mostly in 170..400: the overlap still produces sweeps.
        return commands(8, 1, 1, 0, 229, 90)
                .map(s -> new Spec(s.kind(), s.client(), s.side(), s.type(), s.tif(), s.stp(),
                        s.side() == Side.BUY ? 1 + s.price() : 400 - s.price(), s.quantity(), s.target(),
                        s.asOwner()))
                .list().ofMinSize(200).ofMaxSize(600);
    }

    private static Arbitrary<Spec> commands(int newWeight, int cancelWeight, int replaceWeight,
                                            long minPrice, long maxPrice, int gtcPercent) {
        Arbitrary<CommandType> kind = Arbitraries.frequencyOf(
                Tuple.of(newWeight, Arbitraries.just(CommandType.NEW_ORDER)),
                Tuple.of(cancelWeight, Arbitraries.just(CommandType.CANCEL)),
                Tuple.of(replaceWeight, Arbitraries.just(CommandType.REPLACE)));
        Arbitrary<OrderType> type = Arbitraries.frequency(
                Tuple.of(85, OrderType.LIMIT), Tuple.of(15, OrderType.MARKET));
        int other = (100 - gtcPercent) / 2;
        Arbitrary<TimeInForce> tif = Arbitraries.frequency(
                Tuple.of(gtcPercent, TimeInForce.GTC), Tuple.of(other, TimeInForce.IOC),
                Tuple.of(other, TimeInForce.FOK));
        Arbitrary<Spec> base = Combinators.combine(
                        kind,
                        Arbitraries.integers().between(1, 3),
                        Arbitraries.of(Side.class),
                        type,
                        tif,
                        Arbitraries.of(StpMode.class),
                        Arbitraries.longs().between(minPrice, maxPrice),
                        Arbitraries.longs().between(1, 20))
                .as((k, client, side, t, f, stp, price, qty) ->
                        new Spec(k, client, side, t, f, stp, price, qty, 0, true));
        return Combinators.combine(base, Arbitraries.integers().between(0, 1_000),
                        Arbitraries.of(true, true, true, false))
                .as((b, target, asOwner) -> new Spec(b.kind(), b.client(), b.side(), b.type(), b.tif(),
                        b.stp(), b.price(), b.quantity(), target, asOwner));
    }

    @Property(tries = 300)
    void bookInvariantsHoldAfterEveryCommand(@ForAll("commands") List<Spec> specs) {
        new Checker().run(specs);
    }

    @Property(tries = 100)
    void sameInputProducesSameOutput(@ForAll("commands") List<Spec> specs) {
        assertThat(replayToText(specs)).isEqualTo(replayToText(specs));
    }

    @Property(tries = 100)
    void deepBookInvariantsHold(@ForAll("deepBooks") List<Spec> specs) {
        Checker checker = new Checker();
        checker.run(specs);
        Statistics.label("deep").collect(checker.maxLive >= 50);
        Statistics.label("spans words").collect(checker.maxSpan > 64);
        Statistics.label("deep").coverage(c -> c.check(true).percentage(p -> p > 80));
        Statistics.label("spans words").coverage(c -> c.check(true).percentage(p -> p > 80));
    }

    static List<String> replayToText(List<Spec> specs) {
        EngineDriver driver = new EngineDriver();
        List<String> out = new ArrayList<>();
        List<Long> ids = new ArrayList<>();
        Map<Long, Integer> owners = new HashMap<>();
        Command command = new Command();
        long ts = 0;
        for (Spec s : specs) {
            List<Event> events = driver.process(toCommand(command, s, ++ts, ids, owners));
            recordNewIds(events, ids, owners);
            events.forEach(e -> out.add(e.toString()));
        }
        return out;
    }

    static Command toCommand(Command command, Spec s, long ts, List<Long> ids, Map<Long, Integer> owners) {
        long target = ids.isEmpty() ? 999_999 : ids.get(s.target() % ids.size());
        long client = s.asOwner() && owners.containsKey(target) ? owners.get(target) : s.client();
        return switch (s.kind()) {
            case NEW_ORDER -> command.newOrder(ts, s.client(), ts, EngineDriver.SYMBOL, s.side(), s.type(), s.tif(),
                    s.stp(), s.type() == OrderType.MARKET ? 0 : s.price(), s.quantity());
            case CANCEL -> command.cancel(ts, client, ts, EngineDriver.SYMBOL, target);
            case REPLACE -> command.replace(ts, client, ts, EngineDriver.SYMBOL, target, s.price(), s.quantity());
        };
    }

    static void recordNewIds(List<Event> events, List<Long> ids, Map<Long, Integer> owners) {
        for (Exec e : RecordingSink.execs(events)) {
            if (e.type() == ExecType.NEW) {
                ids.add(e.orderId());
                owners.put(e.orderId(), (int) e.clientId());
            }
        }
    }

    /** Shadow of one order, maintained purely from execution reports. */
    static final class Shadow {
        final long id;
        final long client;
        final Side side;
        final OrderType type;
        final TimeInForce tif;
        long price;
        long quantity;
        long leaves;
        long cum;
        long rank;
        boolean resting;

        Shadow(Exec ack, long rank) {
            this.id = ack.orderId();
            this.client = ack.clientId();
            this.side = ack.side();
            this.type = ack.orderType();
            this.tif = ack.tif();
            this.price = ack.price();
            this.quantity = ack.quantity();
            this.leaves = ack.leaves();
            this.rank = rank;
        }
    }

    static final class Checker {
        private final EngineDriver driver = new EngineDriver();
        private final Map<Long, Shadow> live = new HashMap<>();
        private final Set<Long> done = new HashSet<>();
        private final List<Long> ids = new ArrayList<>();
        private final Map<Long, Integer> owners = new HashMap<>();
        private final Command command = new Command();
        private long makerQuantity;
        private long takerQuantity;
        private long tradeQuantity;
        private long lastSeq;
        int maxLive;
        long maxSpan;

        void run(List<Spec> specs) {
            long k = 0;
            for (Spec s : specs) {
                k++;
                List<Event> events = driver.process(toCommand(command, s, k, ids, owners));
                recordNewIds(events, ids, owners);
                apply(events, k);
                checkAfterCommand();
            }
        }

        private void apply(List<Event> events, long rank) {
            for (Event event : events) {
                assertThat(event.seq()).as("sequence numbers are contiguous").isEqualTo(lastSeq + 1);
                lastSeq = event.seq();
                switch (event) {
                    case Trade t -> {
                        assertThat(t.buyClientId()).as("self-trade prevention").isNotEqualTo(t.sellClientId());
                        tradeQuantity += t.quantity();
                    }
                    case Exec e -> applyExec(e, rank);
                    default -> {
                    }
                }
            }
            // An incoming order that still has quantity after its command must now be resting.
            for (Shadow s : live.values()) {
                if (!s.resting) {
                    assertThat(s.leaves).isPositive();
                    assertThat(s.type).isEqualTo(OrderType.LIMIT);
                    assertThat(s.tif).isEqualTo(TimeInForce.GTC);
                    s.resting = true;
                }
            }
        }

        private void applyExec(Exec e, long rank) {
            switch (e.type()) {
                case NEW -> {
                    assertThat(done).doesNotContain(e.orderId());
                    live.put(e.orderId(), new Shadow(e, rank));
                }
                case REPLACED -> {
                    Shadow s = live.get(e.orderId());
                    assertThat(s).as("replace of a live resting order").isNotNull();
                    assertThat(s.resting).isTrue();
                    boolean keepsPriority = e.price() == s.price && e.leaves() <= s.leaves;
                    if (!keepsPriority) {
                        s.rank = rank;
                        s.resting = false;
                    }
                    s.price = e.price();
                    s.quantity = e.quantity();
                    s.leaves = e.leaves();
                    assertThat(e.cum()).isEqualTo(s.cum);
                }
                case PARTIAL_FILL, FILL -> applyFill(e);
                case CANCELED -> {
                    assertThat(done).as("cancelled at most once").doesNotContain(e.orderId());
                    Shadow s = live.remove(e.orderId());
                    assertThat(s).isNotNull();
                    assertThat(e.leaves()).isZero();
                    assertThat(e.cum()).isEqualTo(s.cum);
                    assertThat(e.quantity()).isGreaterThan(e.cum());
                    done.add(e.orderId());
                }
                case REJECTED, CANCEL_REJECTED, REPLACE_REJECTED -> {
                }
            }
            if (e.type() != ExecType.CANCELED && e.type() != ExecType.REJECTED
                    && e.type() != ExecType.CANCEL_REJECTED && e.type() != ExecType.REPLACE_REJECTED) {
                assertThat(e.quantity()).as("quantity is conserved").isEqualTo(e.cum() + e.leaves());
            }
        }

        private void applyFill(Exec e) {
            assertThat(done).as("cancelled or filled orders never fill").doesNotContain(e.orderId());
            Shadow s = live.get(e.orderId());
            assertThat(s).isNotNull();
            if (e.liquidity() == Liquidity.MAKER) {
                assertThat(s.resting).isTrue();
                assertThat(bestPriority(s.side)).as("price-time priority").isSameAs(s);
                makerQuantity += e.lastQuantity();
            } else {
                assertThat(e.liquidity()).isEqualTo(Liquidity.TAKER);
                assertThat(s.resting).isFalse();
                if (s.type == OrderType.LIMIT) {
                    if (s.side == Side.BUY) {
                        assertThat(e.lastPrice()).isLessThanOrEqualTo(s.price);
                    } else {
                        assertThat(e.lastPrice()).isGreaterThanOrEqualTo(s.price);
                    }
                }
                takerQuantity += e.lastQuantity();
            }
            s.leaves -= e.lastQuantity();
            s.cum += e.lastQuantity();
            assertThat(e.leaves()).isEqualTo(s.leaves);
            assertThat(e.cum()).isEqualTo(s.cum);
            assertThat(e.type()).isEqualTo(s.leaves == 0 ? ExecType.FILL : ExecType.PARTIAL_FILL);
            if (s.leaves == 0) {
                live.remove(s.id);
                done.add(s.id);
            }
        }

        /** The resting order on {@code side} that must trade next: best price, then earliest rank. */
        private Shadow bestPriority(Side side) {
            Shadow best = null;
            for (Shadow s : live.values()) {
                if (!s.resting || s.side != side) {
                    continue;
                }
                if (best == null) {
                    best = s;
                    continue;
                }
                boolean betterPrice = side == Side.BUY ? s.price > best.price : s.price < best.price;
                if (betterPrice || (s.price == best.price && s.rank < best.rank)) {
                    best = s;
                }
            }
            return best;
        }

        private void checkAfterCommand() {
            OrderBook book = driver.book();
            assertThat(book.isCrossed()).as("book is never crossed").isFalse();

            long shadowBestBid = 0;
            long shadowBestAsk = Long.MAX_VALUE;
            long shadowBidQty = 0;
            long shadowAskQty = 0;
            for (Shadow s : live.values()) {
                Order o = driver.engine.restingOrder(s.id);
                assertThat(o).as("shadow order %d is resting in the engine", s.id).isNotNull();
                assertThat(o.leavesQuantity()).isEqualTo(s.leaves);
                assertThat(o.price()).isEqualTo(s.price);
                if (s.side == Side.BUY) {
                    shadowBestBid = Math.max(shadowBestBid, s.price);
                    shadowBidQty += s.leaves;
                } else {
                    shadowBestAsk = Math.min(shadowBestAsk, s.price);
                    shadowAskQty += s.leaves;
                }
            }
            assertThat(shadowBestBid).as("independent not-crossed check").isLessThan(shadowBestAsk);
            maxLive = Math.max(maxLive, live.size());
            long minPrice = live.values().stream().mapToLong(s -> s.price).min().orElse(0);
            long maxPrice = live.values().stream().mapToLong(s -> s.price).max().orElse(0);
            maxSpan = Math.max(maxSpan, maxPrice - minPrice);
            assertThat(driver.engine.restingOrderCount()).isEqualTo(live.size());
            assertThat(book.restingOrders()).isEqualTo(live.size());
            assertThat(book.bestBidPrice()).isEqualTo(shadowBestBid);
            assertThat(book.bestAskPrice()).isEqualTo(shadowBestAsk == Long.MAX_VALUE ? 0 : shadowBestAsk);
            assertThat(sideTotal(book, Side.BUY)).isEqualTo(shadowBidQty);
            assertThat(sideTotal(book, Side.SELL)).isEqualTo(shadowAskQty);

            assertThat(makerQuantity).as("maker and taker fills balance").isEqualTo(takerQuantity);
            assertThat(tradeQuantity).isEqualTo(takerQuantity);
            assertThat(driver.engine.freeOrderCapacity() + live.size())
                    .as("no pooled order leaks").isEqualTo(driver.engine.config().maxOrders());
        }

        private static long sideTotal(OrderBook book, Side side) {
            long total = 0;
            for (PriceLevel l = book.bestLevel(side); l != null; l = book.nextLevel(side, l)) {
                long walked = 0;
                int count = 0;
                for (Order o = l.head(); o != null; o = PriceLevel.nextInQueue(o)) {
                    walked += o.leavesQuantity();
                    count++;
                }
                assertThat(walked).as("level total matches its orders").isEqualTo(l.totalQuantity());
                assertThat(count).isEqualTo(l.orderCount());
                assertThat(count).as("no empty level is marked occupied").isPositive();
                total += walked;
            }
            return total;
        }
    }
}
