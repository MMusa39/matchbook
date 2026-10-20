package dev.matchbook.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** A tight per-client limit: the burst passes, the rest gets 429 with Retry-After, other clients are unaffected. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "matchbook.rate-limit.capacity=5",
        "matchbook.rate-limit.refill-per-second=0.5"})
class RateLimitIT {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        TestEnvironment.register(registry);
    }

    @LocalServerPort
    int port;

    int submit(String key, List<String> retryAfter) {
        var response = RestClient.builder().baseUrl("http://localhost:" + port)
                .defaultStatusHandler(status -> true, (req, res) -> { }).build()
                .post().uri("/api/v1/orders")
                .header("X-API-Key", key).contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("clientOrderId", UUID.randomUUID().toString(), "symbol", "NVDA", "side", "BUY",
                        "type", "LIMIT", "price", "100.00", "quantity", "1"))
                .retrieve().toBodilessEntity();
        String header = response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER);
        if (header != null) {
            retryAfter.add(header);
        }
        return response.getStatusCode().value();
    }

    @Test
    void limitsEachClientIndependently() {
        List<String> retryAfter = new ArrayList<>();
        List<Integer> statuses = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            statuses.add(submit(TestEnvironment.KEY_1, retryAfter));
        }
        assertThat(statuses.subList(0, 5)).containsOnly(201);
        assertThat(statuses.subList(5, 8)).containsOnly(429);
        assertThat(retryAfter).hasSize(3).allSatisfy(h -> assertThat(Integer.parseInt(h)).isBetween(1, 3));

        assertThat(submit(TestEnvironment.KEY_2, retryAfter)).isEqualTo(201);
    }
}
