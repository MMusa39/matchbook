package dev.matchbook.engine.core;

/**
 * Fixed-capacity pool of {@link Order} objects, all allocated at construction. Free orders form an
 * intrusive stack through {@link Order#next}, so acquire and release are O(1) and allocation-free.
 * The pool never grows: when it is empty the engine rejects new orders instead of allocating.
 */
final class OrderPool {

    private final int capacity;
    private Order free;
    private int available;

    OrderPool(int capacity) {
        this.capacity = capacity;
        for (int i = 0; i < capacity; i++) {
            Order order = new Order();
            order.next = free;
            free = order;
        }
        this.available = capacity;
    }

    /** Returns a reset order, or {@code null} if the pool is exhausted. */
    Order acquire() {
        Order order = free;
        if (order == null) {
            return null;
        }
        free = order.next;
        order.next = null;
        available--;
        return order;
    }

    void release(Order order) {
        order.reset();
        order.next = free;
        free = order;
        available++;
    }

    int available() {
        return available;
    }

    int capacity() {
        return capacity;
    }
}
