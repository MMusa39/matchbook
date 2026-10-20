package dev.matchbook.posttrade;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** Upserts symbol reference data into the {@code symbols} table at startup, from the shared symbols.json. */
@Component
public class SymbolSeeder implements ApplicationRunner {

    private final JdbcTemplate jdbc;
    private final JsonMapper json;
    private final String file;

    public SymbolSeeder(JdbcTemplate jdbc, JsonMapper json, @Value("${matchbook.symbols-file:}") String file) {
        this.jdbc = jdbc;
        this.json = json;
        this.file = file;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        JsonNode symbols;
        if (file != null && !file.isBlank()) {
            symbols = json.readTree(Files.readString(Path.of(file)));
        } else {
            try (InputStream in = new ClassPathResource("symbols.json").getInputStream()) {
                symbols = json.readTree(in);
            }
        }
        for (JsonNode s : symbols) {
            jdbc.update("""
                    insert into symbols (symbol_id, symbol, price_scale) values (?, ?, ?)
                    on conflict (symbol_id) do update set symbol = excluded.symbol, price_scale = excluded.price_scale""",
                    s.path("id").asInt(), s.path("symbol").asString(), s.path("priceScale").asLong());
        }
    }
}
