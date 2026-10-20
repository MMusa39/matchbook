package dev.matchbook.engine.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/** Entry point: configure from environment variables, recover, serve until SIGTERM, shut down cleanly. */
public final class EngineServerMain {

    private static final Logger LOG = LoggerFactory.getLogger(EngineServerMain.class);

    private EngineServerMain() {
    }

    public static void main(String[] args) throws InterruptedException {
        EngineServerConfig config = EngineServerConfig.fromEnvironment();
        LOG.info("starting engine: {} symbols, data in {}, wait strategy {}, fsync {}", config.symbols().size(),
                config.dataDirectory(), config.waitStrategy(), config.fsync());
        EngineServer server = new EngineServer(config).start();
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Clean shutdown writes a final snapshot so the next start replays nothing.
            server.close();
            stopped.countDown();
        }, "shutdown"));
        stopped.await();
    }
}
