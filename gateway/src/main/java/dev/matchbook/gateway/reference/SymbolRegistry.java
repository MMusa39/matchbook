package dev.matchbook.gateway.reference;

import dev.matchbook.gateway.config.GatewayProperties;
import org.springframework.beans.factory.annotation.Autowired;
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

/** Loads {@code symbols.json}, the reference data shared with the engine and market data service. */
@Component
public class SymbolRegistry {

    private final Map<String, Symbol> byName;
    private final Map<Integer, Symbol> byId;

    @Autowired
    public SymbolRegistry(GatewayProperties properties) {
        this(load(properties.symbolsFile()));
    }

    public SymbolRegistry(List<Symbol> symbols) {
        this.byName = symbols.stream().collect(Collectors.toUnmodifiableMap(Symbol::symbol, Function.identity()));
        this.byId = symbols.stream().collect(Collectors.toUnmodifiableMap(Symbol::id, Function.identity()));
    }

    public Symbol bySymbol(String symbol) {
        Symbol s = symbol == null ? null : byName.get(symbol);
        if (s == null) {
            throw new InvalidRequestException("unknown symbol: " + symbol);
        }
        return s;
    }

    public Symbol byId(int id) {
        return byId.get(id);
    }

    public List<Symbol> all() {
        return List.copyOf(byId.values());
    }

    static List<Symbol> load(String file) {
        JsonMapper mapper = JsonMapper.builder().build();
        TypeReference<List<Symbol>> type = new TypeReference<>() {
        };
        try {
            if (file != null && !file.isBlank()) {
                return mapper.readValue(Files.readString(Path.of(file)), type);
            }
            try (InputStream in = new ClassPathResource("symbols.json").getInputStream()) {
                return mapper.readValue(in, type);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
