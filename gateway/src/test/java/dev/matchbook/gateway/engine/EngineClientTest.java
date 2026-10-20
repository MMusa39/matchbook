package dev.matchbook.gateway.engine;

import dev.matchbook.proto.v1.EngineCommand;
import dev.matchbook.proto.v1.EngineResult;
import dev.matchbook.proto.v1.MatchingEngineGrpc;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The engine client's timeout and circuit-breaker behaviour against a controllable in-process engine. */
class EngineClientTest {

    /** What the fake engine does for each call. */
    enum Behaviour { OK, UNAVAILABLE, SLOW, BUSY }

    private final AtomicReference<Behaviour> behaviour = new AtomicReference<>(Behaviour.OK);
    private final AtomicInteger calls = new AtomicInteger();
    private Server server;
    private ManagedChannel channel;

    EngineClient start(Duration timeout) throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).addService(new MatchingEngineGrpc.MatchingEngineImplBase() {
            @Override
            public void submit(EngineCommand request, StreamObserver<EngineResult> observer) {
                calls.incrementAndGet();
                switch (behaviour.get()) {
                    case OK -> {
                        observer.onNext(EngineResult.getDefaultInstance());
                        observer.onCompleted();
                    }
                    case UNAVAILABLE -> observer.onError(Status.UNAVAILABLE.asRuntimeException());
                    case BUSY -> observer.onError(Status.RESOURCE_EXHAUSTED.asRuntimeException());
                    case SLOW -> {
                        try {
                            Thread.sleep(timeout.toMillis() * 3);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        observer.onNext(EngineResult.getDefaultInstance());
                        observer.onCompleted();
                    }
                }
            }
        }).build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        return new EngineClient(channel, timeout, new SimpleMeterRegistry());
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void successfulCallsPassThrough() throws Exception {
        EngineClient client = start(Duration.ofSeconds(1));
        assertThat(client.submit(EngineCommand.getDefaultInstance())).isEqualTo(EngineResult.getDefaultInstance());
        assertThat(client.breakerState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void anUnreachableEngineIsDefinitelyNotSentSoARetryIsSafe() throws Exception {
        EngineClient client = start(Duration.ofSeconds(1));
        behaviour.set(Behaviour.UNAVAILABLE);
        assertThatThrownBy(() -> client.submit(EngineCommand.getDefaultInstance()))
                .isInstanceOf(EngineUnavailableException.class)
                .matches(e -> ((EngineUnavailableException) e).definitelyNotSent());
    }

    @Test
    void aFullEngineRingIsDefinitelyNotSent() throws Exception {
        EngineClient client = start(Duration.ofSeconds(1));
        behaviour.set(Behaviour.BUSY);
        assertThatThrownBy(() -> client.submit(EngineCommand.getDefaultInstance()))
                .matches(e -> ((EngineUnavailableException) e).definitelyNotSent());
    }

    @Test
    void aTimeoutIsAnUnknownOutcomeNotARetrySignal() throws Exception {
        EngineClient client = start(Duration.ofMillis(100));
        behaviour.set(Behaviour.SLOW);
        long start = System.nanoTime();
        assertThatThrownBy(() -> client.submit(EngineCommand.getDefaultInstance()))
                .isInstanceOf(EngineUnavailableException.class)
                .matches(e -> !((EngineUnavailableException) e).definitelyNotSent());
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(290));
    }

    @Test
    void repeatedFailuresOpenTheCircuitAndLaterCallsFailFastWithoutReachingTheEngine() throws Exception {
        EngineClient client = start(Duration.ofSeconds(1));
        behaviour.set(Behaviour.UNAVAILABLE);
        for (int i = 0; i < 10; i++) { // minimumNumberOfCalls = 10, failure threshold 50%
            assertThatThrownBy(() -> client.submit(EngineCommand.getDefaultInstance()))
                    .isInstanceOf(EngineUnavailableException.class);
        }
        assertThat(client.breakerState()).isEqualTo(CircuitBreaker.State.OPEN);

        int reached = calls.get();
        behaviour.set(Behaviour.OK);
        assertThatThrownBy(() -> client.submit(EngineCommand.getDefaultInstance()))
                .isInstanceOf(EngineUnavailableException.class)
                .hasMessageContaining("circuit breaker is open")
                .matches(e -> ((EngineUnavailableException) e).definitelyNotSent());
        assertThat(calls.get()).as("an open circuit does not call the engine").isEqualTo(reached);
    }

    @Test
    void businessOutcomesDoNotTripTheBreaker() throws Exception {
        EngineClient client = start(Duration.ofSeconds(1));
        for (int i = 0; i < 30; i++) {
            client.submit(EngineCommand.getDefaultInstance());
        }
        assertThat(client.breakerState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }
}
