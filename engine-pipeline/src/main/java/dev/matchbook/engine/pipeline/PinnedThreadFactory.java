package dev.matchbook.engine.pipeline;

import net.openhft.affinity.AffinityLock;

import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Names Disruptor threads in registration order and optionally pins each one to a CPU with OpenHFT
 * Affinity. Pinning only works on Linux; elsewhere (macOS has no API for it) the request is logged and
 * ignored. On Linux, pinning is only meaningful together with isolating those cores from the scheduler
 * ({@code isolcpus}/{@code nohz_full}) so nothing else runs there.
 */
final class PinnedThreadFactory implements ThreadFactory {

    private static final System.Logger LOG = System.getLogger(PinnedThreadFactory.class.getName());
    private static final boolean LINUX = System.getProperty("os.name", "").toLowerCase().contains("linux");

    private final List<String> names;
    private final List<Integer> cpus;
    private final AtomicInteger next = new AtomicInteger();

    PinnedThreadFactory(List<String> names, List<Integer> cpus) {
        this.names = names;
        this.cpus = cpus;
    }

    @Override
    public Thread newThread(Runnable task) {
        int index = next.getAndIncrement();
        String name = index < names.size() ? names.get(index) : names.getLast() + "-" + index;
        int cpu = index < cpus.size() ? cpus.get(index) : -1;
        Thread thread = new Thread(() -> run(task, name, cpu), name);
        thread.setDaemon(true);
        return thread;
    }

    private static void run(Runnable task, String name, int cpu) {
        if (cpu < 0) {
            task.run();
            return;
        }
        if (!LINUX) {
            LOG.log(System.Logger.Level.INFO, "CPU pinning requested for {0} on CPU {1} but this OS does not "
                    + "support it; running unpinned", name, cpu);
            task.run();
            return;
        }
        try (AffinityLock lock = AffinityLock.acquireLock(cpu)) {
            LOG.log(System.Logger.Level.INFO, "{0} pinned to CPU {1}", name, lock.cpuId());
            task.run();
        }
    }
}
