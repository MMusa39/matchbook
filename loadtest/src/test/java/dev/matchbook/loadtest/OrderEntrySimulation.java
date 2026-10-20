package dev.matchbook.loadtest;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static io.gatling.javaapi.core.CoreDsl.StringBody;
import static io.gatling.javaapi.core.CoreDsl.constantUsersPerSec;
import static io.gatling.javaapi.core.CoreDsl.doIf;
import static io.gatling.javaapi.core.CoreDsl.feed;
import static io.gatling.javaapi.core.CoreDsl.global;
import static io.gatling.javaapi.core.CoreDsl.jsonPath;
import static io.gatling.javaapi.core.CoreDsl.rampUsersPerSec;
import static io.gatling.javaapi.core.CoreDsl.scenario;
import static io.gatling.javaapi.http.HttpDsl.http;
import static io.gatling.javaapi.http.HttpDsl.status;

/**
 * End-to-end order flow through the gateway's REST API: authentication, Redis admission, risk, the
 * engine over gRPC and back. An open injection model (arrivals at a fixed rate, independent of
 * response times) so that a slow system is measured as slow instead of being given fewer requests.
 *
 * <p>Each virtual user is one order-entry session: it places a passive limit order, sometimes an
 * aggressive one that trades, and cancels its passive order if it is still resting.
 *
 * <pre>./mvnw -pl loadtest gatling:test -Dgateway=http://localhost:8080 -Drate=200 -Dduration=60</pre>
 */
public class OrderEntrySimulation extends Simulation {

    private static final String GATEWAY = System.getProperty("gateway", "http://localhost:8080");
    private static final double RATE = Double.parseDouble(System.getProperty("rate", "200"));
    private static final int DURATION = Integer.parseInt(System.getProperty("duration", "60"));
    private static final String[] KEYS = {"local-dev-key-1", "local-dev-key-2", "local-dev-key-3"};
    private static final String[] SYMBOLS = {"AAPL", "MSFT", "NVDA", "TSLA"};
    private static final Map<String, Integer> MID = Map.of("AAPL", 25_000, "MSFT", 45_000, "NVDA", 15_000, "TSLA", 30_000);

    private static String price(int cents) {
        return (cents / 100) + "." + String.format("%02d", cents % 100);
    }

    private final Iterator<Map<String, Object>> orders = Stream.generate(() -> {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        String symbol = SYMBOLS[r.nextInt(SYMBOLS.length)];
        int mid = MID.get(symbol);
        boolean buy = r.nextBoolean();
        int passive = buy ? mid - 1 - r.nextInt(50) : mid + 1 + r.nextInt(50);
        int aggressive = buy ? mid + 60 : mid - 60;
        return Map.<String, Object>ofEntries(
                Map.entry("apiKey", KEYS[r.nextInt(KEYS.length)]),
                Map.entry("symbol", symbol),
                Map.entry("side", buy ? "BUY" : "SELL"),
                Map.entry("otherSide", buy ? "SELL" : "BUY"),
                Map.entry("passivePrice", price(passive)),
                Map.entry("aggressivePrice", price(aggressive)),
                Map.entry("quantity", 1 + r.nextInt(100)),
                Map.entry("aggressive", r.nextInt(100) < 30),
                Map.entry("passiveId", UUID.randomUUID().toString()),
                Map.entry("aggressiveId", UUID.randomUUID().toString()),
                Map.entry("cancelId", UUID.randomUUID().toString()));
    }).iterator();

    private final HttpProtocolBuilder protocol = http.baseUrl(GATEWAY)
            .contentTypeHeader("application/json")
            .acceptHeader("application/json")
            .shareConnections();

    private final ScenarioBuilder session = scenario("order entry session")
            .exec(feed(orders))
            .exec(http("submit passive limit").post("/api/v1/orders")
                    .header("X-API-Key", "#{apiKey}")
                    .body(StringBody("""
                            {"clientOrderId":"#{passiveId}","symbol":"#{symbol}","side":"#{side}","type":"LIMIT",
                             "price":"#{passivePrice}","quantity":"#{quantity}"}"""))
                    .check(status().in(201, 422))
                    .check(jsonPath("$.orderId").saveAs("orderId"))
                    .check(jsonPath("$.status").saveAs("status")))
            .exec(doIf("#{aggressive}").then(
                    http("submit aggressive IOC").post("/api/v1/orders")
                            .header("X-API-Key", "#{apiKey}")
                            .body(StringBody("""
                                    {"clientOrderId":"#{aggressiveId}","symbol":"#{symbol}","side":"#{otherSide}",
                                     "type":"LIMIT","timeInForce":"IOC","price":"#{aggressivePrice}",
                                     "quantity":"#{quantity}"}"""))
                            .check(status().in(201, 422))))
            .exec(doIf(s -> "NEW".equals(s.getString("status"))).then(
                    http("cancel").post("/api/v1/orders/#{orderId}/cancel")
                            .header("X-API-Key", "#{apiKey}")
                            .body(StringBody("{\"clientOrderId\":\"#{cancelId}\",\"symbol\":\"#{symbol}\"}"))
                            .check(status().in(200, 422))));

    {
        setUp(session.injectOpen(
                rampUsersPerSec(1).to(RATE).during(Duration.ofSeconds(10)),
                constantUsersPerSec(RATE).during(Duration.ofSeconds(DURATION))))
                .protocols(protocol)
                .assertions(
                        global().successfulRequests().percent().gte(99.0),
                        global().responseTime().percentile(99.0).lt(250));
    }
}
