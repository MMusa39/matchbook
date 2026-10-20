package dev.matchbook.marketdata;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Current book snapshot over HTTP, for quick inspection. Streaming clients should use the WebSocket. */
@RestController
public class BookController {

    private final MarketDataHub hub;

    public BookController(MarketDataHub hub) {
        this.hub = hub;
    }

    @GetMapping("/api/v1/book/{symbol}")
    public ResponseEntity<Map<String, Object>> book(@PathVariable String symbol) {
        Map<String, Object> snapshot = hub.snapshot(symbol);
        return snapshot == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(snapshot);
    }
}
