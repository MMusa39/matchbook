package dev.matchbook.loadtest.bots;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Minimal REST client for the gateway, JDK only. */
final class GatewayClient {

    private static final Pattern ORDER_ID = Pattern.compile("\"orderId\"\\s*:\\s*(\\d+)");
    private static final Pattern STATUS = Pattern.compile("\"status\"\\s*:\\s*\"([A-Z_]+)\"");

    record Result(int httpStatus, long orderId, String status) {
    }

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final String baseUrl;
    private final String apiKey;

    GatewayClient(String baseUrl, String apiKey) {
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
    }

    Result limit(String symbol, String side, String price, long quantity, String tif) {
        return post("/api/v1/orders", """
                {"clientOrderId":"%s","symbol":"%s","side":"%s","type":"LIMIT","timeInForce":"%s",
                 "price":"%s","quantity":"%d"}""".formatted(UUID.randomUUID(), symbol, side, tif, price, quantity));
    }

    Result market(String symbol, String side, long quantity) {
        return post("/api/v1/orders", """
                {"clientOrderId":"%s","symbol":"%s","side":"%s","type":"MARKET","quantity":"%d"}"""
                .formatted(UUID.randomUUID(), symbol, side, quantity));
    }

    Result cancel(String symbol, long orderId) {
        return post("/api/v1/orders/" + orderId + "/cancel",
                "{\"clientOrderId\":\"%s\",\"symbol\":\"%s\"}".formatted(UUID.randomUUID(), symbol));
    }

    private Result post(String path, String body) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .header("X-API-Key", apiKey)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            Matcher id = ORDER_ID.matcher(response.body());
            Matcher status = STATUS.matcher(response.body());
            return new Result(response.statusCode(), id.find() ? Long.parseLong(id.group(1)) : 0,
                    status.find() ? status.group(1) : "");
        } catch (IOException e) {
            return new Result(0, 0, "IO_ERROR");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Result(0, 0, "INTERRUPTED");
        }
    }
}
