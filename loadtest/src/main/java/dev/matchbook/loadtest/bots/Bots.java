package dev.matchbook.loadtest.bots;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Background flow for demos and dashboards: one market maker quoting every symbol around a drifting fair
 * value, and noise traders that hit those quotes at random.
 *
 * <pre>java -cp loadtest/target/classes dev.matchbook.loadtest.bots.Bots http://localhost:8080</pre>
 */
public final class Bots {

    private static final Map<String, Long> START_CENTS = Map.of("AAPL", 25_000L, "MSFT", 45_000L, "NVDA", 15_000L,
            "TSLA", 30_000L);

    private Bots() {
    }

    static String price(long cents) {
        return cents / 100 + "." + String.format("%02d", cents % 100);
    }

    public static void main(String[] args) throws InterruptedException {
        String gateway = args.length > 0 ? args[0] : "http://localhost:8080";
        Map<String, Long> fair = new ConcurrentHashMap<>(START_CENTS);
        List<Thread> threads = new ArrayList<>();
        threads.add(Thread.ofVirtual().name("market-maker").start(() -> marketMaker(gateway, fair)));
        threads.add(Thread.ofVirtual().name("noise-1").start(() -> noise(gateway, "local-dev-key-1", fair)));
        threads.add(Thread.ofVirtual().name("noise-2").start(() -> noise(gateway, "local-dev-key-2", fair)));
        System.out.println("bots running against " + gateway + " (Ctrl-C to stop)");
        for (Thread t : threads) {
            t.join();
        }
    }

    /** Quotes 3 levels a side around fair value; requotes every 500 ms after the fair value drifts. */
    static void marketMaker(String gateway, Map<String, Long> fair) {
        GatewayClient client = new GatewayClient(gateway, "local-dev-key-3");
        Map<String, List<Long>> resting = new ConcurrentHashMap<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        while (!Thread.currentThread().isInterrupted()) {
            for (String symbol : START_CENTS.keySet()) {
                for (long id : resting.getOrDefault(symbol, List.of())) {
                    client.cancel(symbol, id);
                }
                long center = fair.merge(symbol, (long) random.nextInt(-5, 6), Long::sum);
                center = Math.max(START_CENTS.get(symbol) * 8 / 10, Math.min(START_CENTS.get(symbol) * 12 / 10, center));
                fair.put(symbol, center);
                List<Long> ids = new ArrayList<>();
                for (int level = 1; level <= 3; level++) {
                    long size = 100L * level;
                    var bid = client.limit(symbol, "BUY", price(center - 5L * level), size, "GTC");
                    var ask = client.limit(symbol, "SELL", price(center + 5L * level), size, "GTC");
                    if (bid.orderId() > 0 && "NEW".equals(bid.status())) {
                        ids.add(bid.orderId());
                    }
                    if (ask.orderId() > 0 && "NEW".equals(ask.status())) {
                        ids.add(ask.orderId());
                    }
                }
                resting.put(symbol, ids);
            }
            sleep(500);
        }
    }

    /** Sends a small marketable order every 50-250 ms. */
    static void noise(String gateway, String apiKey, Map<String, Long> fair) {
        GatewayClient client = new GatewayClient(gateway, apiKey);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<String> symbols = List.copyOf(START_CENTS.keySet());
        while (!Thread.currentThread().isInterrupted()) {
            String symbol = symbols.get(random.nextInt(symbols.size()));
            String side = random.nextBoolean() ? "BUY" : "SELL";
            long quantity = 1 + random.nextInt(50);
            if (random.nextInt(4) == 0) {
                client.market(symbol, side, quantity);
            } else {
                long center = fair.get(symbol);
                long limit = "BUY".equals(side) ? center + 10 : center - 10;
                client.limit(symbol, side, price(limit), quantity, "IOC");
            }
            sleep(50 + random.nextInt(200));
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
