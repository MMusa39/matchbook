package dev.matchbook.engine.core;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Golden-file scenarios: {@code src/test/resources/scenarios/*.txt}. Each file interleaves commands
 * ({@code > ...}) with the exact events expected after each one ({@code < ...}), so a reviewer reads
 * cause and effect together. Lines starting with {@code #} are comments.
 *
 * <p>Command syntax: {@code > NEW client=1 side=BUY px=100 qty=10} with optional {@code type=}
 * (LIMIT), {@code tif=} (GTC), {@code stp=} (CANCEL_RESTING), {@code sym=} (1), {@code cl=}
 * (auto); {@code > CANCEL client=1 oid=1}; {@code > REPLACE client=1 oid=1 px=101 qty=5}.
 * Timestamps auto-increment per command. An optional
 * {@code SYMBOL id=1 tick=1 lot=1 min=1 max=1000000 low=1 high=10000} line overrides the default symbol.
 *
 * <p>To regenerate the expected events after an intentional behaviour change, run with
 * {@code -Dgolden.update=true} and review the diff before committing.
 */
class GoldenScenarioTest {

    private static final Path DIR = Path.of("src/test/resources/scenarios");
    private static final boolean UPDATE = Boolean.getBoolean("golden.update");

    @TestFactory
    Stream<DynamicTest> scenarios() throws IOException {
        List<Path> files;
        try (Stream<Path> list = Files.list(DIR)) {
            files = list.filter(p -> p.toString().endsWith(".txt")).sorted().toList();
        }
        assertThat(files).isNotEmpty();
        return files.stream().map(f -> DynamicTest.dynamicTest(f.getFileName().toString(), () -> run(f)));
    }

    private static void run(Path file) {
        List<String> lines = read(file);
        List<String> expected = new ArrayList<>();
        List<String> actual = new ArrayList<>();
        List<String> rewritten = new ArrayList<>();

        SymbolConfig symbol = EngineDriver.testSymbol(1);
        for (String line : lines) {
            if (line.startsWith("SYMBOL ")) {
                symbol = parseSymbol(line);
            }
        }
        MatchingEngine engine;
        RecordingSink sink = new RecordingSink();
        engine = new MatchingEngine(new EngineConfig(0, 1_000, List.of(symbol)), sink);
        Command command = new Command();
        long ts = 0;
        long clOrdId = 0;

        for (String line : lines) {
            if (line.startsWith("< ")) {
                expected.add(line.substring(2));
                continue;
            }
            rewritten.add(line);
            if (!line.startsWith("> ")) {
                continue;
            }
            Map<String, String> f = fields(line.substring(2));
            String kind = f.get("$kind");
            ts++;
            long cl = f.containsKey("cl") ? Long.parseLong(f.get("cl")) : ++clOrdId;
            int sym = Integer.parseInt(f.getOrDefault("sym", String.valueOf(symbol.symbolId())));
            long client = Long.parseLong(f.get("client"));
            switch (kind) {
                case "NEW" -> command.newOrder(ts, client, cl, sym, Side.valueOf(f.get("side")),
                        OrderType.valueOf(f.getOrDefault("type", "LIMIT")),
                        TimeInForce.valueOf(f.getOrDefault("tif", "GTC")),
                        StpMode.valueOf(f.getOrDefault("stp", "CANCEL_RESTING")),
                        Long.parseLong(f.getOrDefault("px", "0")), Long.parseLong(f.get("qty")));
                case "CANCEL" -> command.cancel(ts, client, cl, sym, Long.parseLong(f.get("oid")));
                case "REPLACE" -> command.replace(ts, client, cl, sym, Long.parseLong(f.get("oid")),
                        Long.parseLong(f.get("px")), Long.parseLong(f.get("qty")));
                default -> throw new IllegalArgumentException("unknown command in " + file + ": " + line);
            }
            engine.process(command);
            for (RecordingSink.Event e : sink.drain()) {
                actual.add(e.toString());
                rewritten.add("< " + e);
            }
        }

        if (UPDATE) {
            write(file, rewritten);
            return;
        }
        assertThat(actual).as(file.toString()).containsExactlyElementsOf(expected);
    }

    private static Map<String, String> fields(String text) {
        String[] tokens = text.trim().split("\\s+");
        Map<String, String> map = new HashMap<>();
        map.put("$kind", tokens[0]);
        for (int i = 1; i < tokens.length; i++) {
            int eq = tokens[i].indexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException("expected key=value: " + tokens[i]);
            }
            map.put(tokens[i].substring(0, eq), tokens[i].substring(eq + 1));
        }
        return map;
    }

    private static SymbolConfig parseSymbol(String line) {
        Map<String, String> f = fields(line);
        return new SymbolConfig(Integer.parseInt(f.get("id")), "SYM" + f.get("id"), 100,
                Long.parseLong(f.get("tick")), Long.parseLong(f.get("lot")), Long.parseLong(f.get("min")),
                Long.parseLong(f.get("max")), Long.parseLong(f.get("low")), Long.parseLong(f.get("high")));
    }

    private static List<String> read(Path file) {
        try {
            return Files.readAllLines(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void write(Path file, List<String> lines) {
        try {
            Files.write(file, lines);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
