package dev.matchbook.gateway.admission;

import dev.matchbook.gateway.config.GatewayProperties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class RedisAdmissionIT {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:8.8.3-alpine").withExposedPorts(6379);

    static StringRedisTemplate redis;

    @BeforeAll
    static void connect() {
        LettuceConnectionFactory factory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        factory.afterPropertiesSet();
        redis = new StringRedisTemplate(factory);
        redis.afterPropertiesSet();
    }

    static RedisAdmission admission(int capacity, double refillPerSecond) {
        return new RedisAdmission(redis, new GatewayProperties("x", Duration.ofSeconds(1), "", Map.of(),
                new GatewayProperties.RateLimit(capacity, refillPerSecond), Duration.ofMinutes(5),
                new GatewayProperties.Risk(1, BigDecimal.ONE, 1, BigDecimal.ONE)));
    }

    @Test
    void assignsIncreasingInternalIdsAndRemembersTheExternalOnes() {
        RedisAdmission admission = admission(100, 100);
        Admission a = admission.admit(101, "order-a");
        Admission b = admission.admit(101, "order-b");
        assertThat(a).isInstanceOf(Admission.Admitted.class);
        long idA = ((Admission.Admitted) a).internalClOrdId();
        long idB = ((Admission.Admitted) b).internalClOrdId();
        assertThat(idB).isEqualTo(idA + 1);
        assertThat(admission.externalClOrdId(101, idB)).isEqualTo("order-b");
    }

    @Test
    void duplicateIsInProgressUntilCompletedThenReplaysTheStoredOutcome() {
        RedisAdmission admission = admission(100, 100);
        admission.admit(102, "dup");
        assertThat(admission.admit(102, "dup")).isEqualTo(new Admission.InProgress(false));
        admission.complete(102, "dup", "{\"x\":1}");
        assertThat(admission.admit(102, "dup")).isEqualTo(new Admission.Completed("{\"x\":1}"));
        admission.markUnknown(102, "dup");
        assertThat(admission.admit(102, "dup")).isEqualTo(new Admission.InProgress(true));
        admission.release(102, "dup");
        assertThat(admission.admit(102, "dup")).isInstanceOf(Admission.Admitted.class);
    }

    @Test
    void tokenBucketLimitsBurstsAndRefillsOverTime() throws InterruptedException {
        RedisAdmission admission = admission(5, 10); // 5-token burst, 10 tokens/s refill
        for (int i = 0; i < 5; i++) {
            assertThat(admission.admit(103, "burst-" + i)).isInstanceOf(Admission.Admitted.class);
        }
        Admission limited = admission.admit(103, "burst-5");
        assertThat(limited).isInstanceOf(Admission.RateLimited.class);
        assertThat(((Admission.RateLimited) limited).retryAfterMillis()).isBetween(1L, 100L);

        Thread.sleep(250); // ~2.5 tokens refill
        assertThat(admission.admit(103, "later-1")).isInstanceOf(Admission.Admitted.class);
        assertThat(admission.admit(103, "later-2")).isInstanceOf(Admission.Admitted.class);
    }

    @Test
    void duplicatesDoNotConsumeTokens() {
        RedisAdmission admission = admission(2, 0.001);
        admission.admit(104, "one");
        for (int i = 0; i < 10; i++) {
            assertThat(admission.admit(104, "one")).isInstanceOf(Admission.InProgress.class);
        }
        assertThat(admission.admit(104, "two")).isInstanceOf(Admission.Admitted.class);
    }

    @Test
    void concurrentAdmissionsNeverExceedTheBucketAndNeverShareAnId() throws Exception {
        RedisAdmission admission = admission(50, 0.001);
        ExecutorService pool = Executors.newFixedThreadPool(16);
        List<Future<Admission>> results = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            String id = "c-" + i;
            results.add(pool.submit(() -> admission.admit(105, id)));
        }
        List<Long> ids = new ArrayList<>();
        int limited = 0;
        for (Future<Admission> f : results) {
            switch (f.get()) {
                case Admission.Admitted a -> ids.add(a.internalClOrdId());
                case Admission.RateLimited r -> limited++;
                default -> throw new AssertionError();
            }
        }
        pool.shutdown();
        assertThat(ids).hasSize(50).doesNotHaveDuplicates();
        assertThat(limited).isEqualTo(150);
    }
}
