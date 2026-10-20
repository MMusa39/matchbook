package dev.matchbook.marketdata;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Loads symbol reference data from {@code matchbook.symbols-file}, or the bundled {@code symbols.json}. */
@Component
public class SymbolCatalog {

    private final List<SymbolInfo> symbols;

    public SymbolCatalog(@Value("${matchbook.symbols-file:}") String file) {
        JsonMapper mapper = JsonMapper.builder().build();
        TypeReference<List<SymbolInfo>> type = new TypeReference<>() {
        };
        try {
            if (file != null && !file.isBlank()) {
                symbols = mapper.readValue(Files.readString(Path.of(file)), type);
            } else {
                try (InputStream in = new ClassPathResource("symbols.json").getInputStream()) {
                    symbols = mapper.readValue(in, type);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Map<Integer, SymbolInfo> byId() {
        return symbols.stream().collect(Collectors.toUnmodifiableMap(SymbolInfo::id, Function.identity()));
    }

    public Map<String, SymbolInfo> byName() {
        return symbols.stream().collect(Collectors.toUnmodifiableMap(SymbolInfo::symbol, Function.identity()));
    }
}
