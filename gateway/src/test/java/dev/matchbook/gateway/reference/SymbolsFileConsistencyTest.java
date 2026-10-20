package dev.matchbook.gateway.reference;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** The bundled reference data must match the engine's copy, or prices would be scaled differently. */
class SymbolsFileConsistencyTest {

    @Test
    void gatewayAndEngineShipTheSameSymbols() throws Exception {
        String gateway = Files.readString(Path.of("src/main/resources/symbols.json"));
        String engine = Files.readString(Path.of("../engine-server/src/main/resources/symbols.json"));
        assertThat(gateway).isEqualTo(engine);
    }
}
