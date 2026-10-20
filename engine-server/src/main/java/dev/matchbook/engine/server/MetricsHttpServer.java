package dev.matchbook.engine.server;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;

/** Minimal HTTP endpoint for Prometheus scraping and Kubernetes probes: {@code /metrics}, {@code /health}. */
final class MetricsHttpServer implements AutoCloseable {

    private final HttpServer server;

    MetricsHttpServer(int port, PrometheusMeterRegistry registry, BooleanSupplier healthy) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.createContext("/health", exchange -> {
            boolean ok = healthy.getAsBoolean();
            byte[] body = (ok ? "UP" : "DOWN").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(ok ? 200 : 503, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
