package dev.matchbook.gateway.engine;

import dev.matchbook.gateway.config.GatewayProperties;
import dev.matchbook.proto.v1.EngineCommand;
import dev.matchbook.proto.v1.EngineResult;
import dev.matchbook.proto.v1.MatchingEngineGrpc;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Calls the matching engine over gRPC.
 *
 * <ul>
 *     <li><b>Timeout:</b> a gRPC deadline per call. Unlike a client-side timer, a deadline is propagated
 *     to the server, which abandons the call too.</li>
 *     <li><b>Circuit breaker</b> (Resilience4j): when most recent calls fail, stop calling for a while
 *     and reject immediately, instead of piling requests onto an engine that is down or overloaded.
 *     Engine business rejects are successful calls and do not count.</li>
 *     <li><b>No retries:</b> a resubmitted order is not idempotent at the engine. Retry safety is
 *     provided one level up by the client order id (ADR-0012).</li>
 * </ul>
 */
@Component
public class EngineClient {

    private final ManagedChannel channel;
    private final MatchingEngineGrpc.MatchingEngineBlockingStub stub;
    private final CircuitBreaker breaker;
    private final Duration timeout;

    @Autowired
    public EngineClient(GatewayProperties properties, MeterRegistry meters) {
        this(NettyChannelBuilder.forTarget(properties.engineTarget())
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .build(), properties.engineTimeout(), meters);
    }

    /** For tests: talk to the engine over a caller-supplied channel. */
    public EngineClient(ManagedChannel channel, Duration timeout, MeterRegistry meters) {
        this.channel = channel;
        this.stub = MatchingEngineGrpc.newBlockingStub(channel);
        this.timeout = timeout;
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(50)
                .minimumNumberOfCalls(10)
                .failureRateThreshold(50)
                .slowCallDurationThreshold(timeout.dividedBy(2))
                .slowCallRateThreshold(80)
                .waitDurationInOpenState(Duration.ofSeconds(5))
                .permittedNumberOfCallsInHalfOpenState(5)
                .recordException(e -> e instanceof StatusRuntimeException s && isInfrastructureFailure(s))
                .build());
        this.breaker = registry.circuitBreaker("engine");
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry).bindTo(meters);
    }

    public ManagedChannel channel() {
        return channel;
    }

    public EngineResult submit(EngineCommand command) {
        try {
            return breaker.executeSupplier(() ->
                    stub.withDeadlineAfter(timeout.toMillis(), TimeUnit.MILLISECONDS).submit(command));
        } catch (CallNotPermittedException e) {
            throw new EngineUnavailableException("engine circuit breaker is open", true, e);
        } catch (StatusRuntimeException e) {
            Status.Code code = e.getStatus().getCode();
            if (code == Status.Code.RESOURCE_EXHAUSTED || code == Status.Code.UNAVAILABLE) {
                // UNAVAILABLE: no connection, the request was not delivered. RESOURCE_EXHAUSTED: ring full.
                throw new EngineUnavailableException("engine unavailable: " + code, true, e);
            }
            if (code == Status.Code.DEADLINE_EXCEEDED) {
                throw new EngineUnavailableException("engine did not answer within " + timeout, false, e);
            }
            throw e;
        }
    }

    public CircuitBreaker.State breakerState() {
        return breaker.getState();
    }

    private static boolean isInfrastructureFailure(StatusRuntimeException e) {
        return switch (e.getStatus().getCode()) {
            case UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, INTERNAL, UNKNOWN -> true;
            default -> false;
        };
    }

    @PreDestroy
    void close() {
        channel.shutdownNow();
    }
}
