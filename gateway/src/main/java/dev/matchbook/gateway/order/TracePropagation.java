package dev.matchbook.gateway.order;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/**
 * Produces the W3C {@code traceparent} of the current span, which travels inside the engine command so
 * the engine can attach its Kafka records to the same trace (the engine's rings are an asynchronous hop
 * that no instrumentation agent can follow). Returns {@code null} when tracing is not configured.
 */
@Component
public class TracePropagation {

    private final ObjectProvider<Tracer> tracer;

    public TracePropagation(ObjectProvider<Tracer> tracer) {
        this.tracer = tracer;
    }

    public String currentTraceparent() {
        Tracer t = tracer.getIfAvailable();
        if (t == null) {
            return null;
        }
        Span span = t.currentSpan();
        if (span == null) {
            return null;
        }
        var context = span.context();
        String traceId = context.traceId();
        String spanId = context.spanId();
        if (traceId == null || traceId.length() != 32 || spanId == null || spanId.length() != 16) {
            return null;
        }
        boolean sampled = Boolean.TRUE.equals(context.sampled());
        return "00-" + traceId + "-" + spanId + (sampled ? "-01" : "-00");
    }
}
