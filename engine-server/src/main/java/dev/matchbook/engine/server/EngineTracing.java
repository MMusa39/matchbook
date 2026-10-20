package dev.matchbook.engine.server;

import dev.matchbook.engine.pipeline.OutboundEvent;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import java.util.concurrent.TimeUnit;

/**
 * OpenTelemetry for the engine process. The matching thread never touches it: spans are created on an
 * outbound listener thread from the timestamps and trace context that travel with each event.
 *
 * <p>A command that arrived with a sampled W3C trace context gets one {@code engine.process} span, a
 * child of the caller's span, from ingress to the command's last event. Kafka records of that command
 * carry the engine span as their parent, so post-trade's consumer span continues the same trace:
 * gateway → engine → post-trade.
 */
final class EngineTracing implements AutoCloseable {

    private static final AttributeKey<Long> SHARD = AttributeKey.longKey("matchbook.shard");
    private static final AttributeKey<Long> JOURNAL_SEQ = AttributeKey.longKey("matchbook.journal_seq");
    private static final AttributeKey<Long> SYMBOL = AttributeKey.longKey("matchbook.symbol_id");

    private final OpenTelemetrySdk sdk;
    private final Tracer tracer;
    /** Converts System.nanoTime() readings to epoch nanoseconds. */
    private final long epochOffsetNanos;

    private EngineTracing(OpenTelemetrySdk sdk) {
        this.sdk = sdk;
        this.tracer = sdk == null ? null : sdk.getTracer("dev.matchbook.engine");
        this.epochOffsetNanos = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis()) - System.nanoTime();
    }

    /** Exports over OTLP/HTTP when {@code OTEL_EXPORTER_OTLP_ENDPOINT} is set; otherwise does nothing. */
    static EngineTracing fromEnvironment() {
        String endpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
        if (endpoint == null || endpoint.isBlank()) {
            return new EngineTracing(null);
        }
        String service = System.getenv().getOrDefault("OTEL_SERVICE_NAME", "engine");
        SdkTracerProvider provider = SdkTracerProvider.builder()
                .setResource(Resource.getDefault().merge(Resource.create(
                        Attributes.of(AttributeKey.stringKey("service.name"), service))))
                .addSpanProcessor(BatchSpanProcessor.builder(OtlpHttpSpanExporter.builder()
                        .setEndpoint(endpoint.replaceAll("/$", "") + "/v1/traces").build()).build())
                .build();
        return new EngineTracing(OpenTelemetrySdk.builder().setTracerProvider(provider).build());
    }

    static EngineTracing disabled() {
        return new EngineTracing(null);
    }

    /** Starts the span for a command if it carries a sampled trace context; {@code null} otherwise. */
    Span start(OutboundEvent event, int shard) {
        if (tracer == null || (event.traceIdHigh() == 0 && event.traceIdLow() == 0)
                || (event.traceFlags() & 0x01) == 0) {
            return null;
        }
        SpanContext parent = SpanContext.createFromRemoteParent(
                hex(event.traceIdHigh()) + hex(event.traceIdLow()), hex(event.spanId()), TraceFlags.getSampled(),
                TraceState.getDefault());
        return tracer.spanBuilder("engine.process")
                .setParent(Context.root().with(Span.wrap(parent)))
                .setSpanKind(SpanKind.INTERNAL)
                .setStartTimestamp(event.ingressNanos() + epochOffsetNanos, TimeUnit.NANOSECONDS)
                .setAttribute(SHARD, (long) shard)
                .setAttribute(JOURNAL_SEQ, event.journalSeq())
                .setAttribute(SYMBOL, (long) event.symbolId())
                .startSpan();
    }

    void end(Span span) {
        span.end(System.nanoTime() + epochOffsetNanos, TimeUnit.NANOSECONDS);
    }

    static String traceparent(Span span) {
        SpanContext c = span.getSpanContext();
        return "00-" + c.getTraceId() + "-" + c.getSpanId() + "-" + c.getTraceFlags().asHex();
    }

    private static String hex(long value) {
        String h = Long.toHexString(value);
        return "0".repeat(16 - h.length()) + h;
    }

    @Override
    public void close() {
        if (sdk != null) {
            sdk.close();
        }
    }
}
