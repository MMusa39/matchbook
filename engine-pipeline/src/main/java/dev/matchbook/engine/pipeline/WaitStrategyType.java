package dev.matchbook.engine.pipeline;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;

/**
 * How a pipeline stage waits for work. The trade-off is latency against CPU:
 * <ul>
 *     <li>{@link #BUSY_SPIN}: spins on the sequence with {@code Thread.onSpinWait()}. Lowest and most
 *     stable latency; burns a full core per stage forever. Only sensible with a dedicated (ideally
 *     isolated and pinned) core per stage.</li>
 *     <li>{@link #YIELDING}: spins briefly, then {@code Thread.yield()}. Near busy-spin latency while
 *     letting other runnable threads in; still ~100% CPU when idle.</li>
 *     <li>{@link #SLEEPING}: spins, yields, then parks for ~100µs. Low idle CPU; adds wake-up latency
 *     after quiet periods.</li>
 *     <li>{@link #BLOCKING}: lock and condition variable. Lowest CPU; highest and least predictable
 *     latency because every hand-off can involve a futex wake.</li>
 * </ul>
 */
public enum WaitStrategyType {
    BUSY_SPIN,
    YIELDING,
    SLEEPING,
    BLOCKING;

    public WaitStrategy create() {
        return switch (this) {
            case BUSY_SPIN -> new BusySpinWaitStrategy();
            case YIELDING -> new YieldingWaitStrategy();
            case SLEEPING -> new SleepingWaitStrategy();
            case BLOCKING -> new BlockingWaitStrategy();
        };
    }
}
