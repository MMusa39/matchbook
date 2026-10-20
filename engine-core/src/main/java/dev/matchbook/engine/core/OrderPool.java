package dev.matchbook.engine.core;

/**
 * Fixed-capacity pool of {@link Order} objects, all allocated at construction. Free orders form an
 * intrusive stack through {@link Order#next}, so acquire and release are O(1) and allocation-free.
 * The pool never grows: when it is empty the engine rejects new orders instead of allocating.
 *
 * <p>With {@code pooling == false} it keeps the same capacity accounting but allocates a fresh order on
 * every acquire and drops released ones. That mode only exists to benchmark the difference.
 */
final class OrderPool {

    private final int capacity;
    private final boolean pooling;
    private Order free;
    private int available;

    OrderPool(int capacity, boolean pooling) {
        this.capacity = capacity;
        this.pooling = pooling;
        if (pooling) {
            for (int i = 0; i < capacity; i++) {
                Order order = new Order();
                order.next = free;
                free = order;
            }
        }
        this.available = capacity;
    }

    /** Returns a reset order, or {@code null} if the pool is exhausted. */
    Order acquire() {
        if (available == 0) {
            return null;
        }
        available--;
        if (!pooling) {
            return new Order();
        }
        Order order = free;
        free = order.next;
        order.next = null;
        return order;
    }

    void release(Order order) {
        available++;
        if (!pooling) {
            return;
        }
        order.reset();
        order.next = free;
        free = order;
    }

    int available() {
        return available;
    }

    int capacity() {
        return capacity;
    }
}
