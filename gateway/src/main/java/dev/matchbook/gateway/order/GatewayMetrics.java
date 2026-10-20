package dev.matchbook.gateway.order;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Gateway metrics:
 * {@code matchbook_gateway_requests_total{kind,result}},
 * {@code matchbook_gateway_rejects_total{kind,reason}} and
 * {@code matchbook_gateway_request_seconds{kind}} (histogram, end to end inside the gateway).
 */
@Component
public class GatewayMetrics {

    private final MeterRegistry registry;

    public GatewayMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    Timer.Sample start() {
        return Timer.start(registry);
    }

    void record(Timer.Sample sample, String kind, OrderOutcome outcome) {
        sample.stop(Timer.builder("matchbook.gateway.request")
                .tag("kind", kind)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofNanos(50_000))
                .maximumExpectedValue(Duration.ofSeconds(5))
                .register(registry));
        Counter.builder("matchbook.gateway.requests").tag("kind", kind).tag("result", outcome.result().name())
                .register(registry).increment();
        if (!outcome.accepted() && outcome.reason() != null && !outcome.reason().isBlank()) {
            Counter.builder("matchbook.gateway.rejects").tag("kind", kind).tag("reason", outcome.reason())
                    .register(registry).increment();
        }
    }
}
