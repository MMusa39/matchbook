package dev.matchbook.engine.core;

/**
 * All resting orders at one price on one side, in time priority: {@link #head} is the oldest.
 * Append, remove and pop-front are O(1) because the queue links live in {@link Order} itself.
 * Levels are pre-allocated for every price in the band and never freed.
 */
public final class PriceLevel {

    final long price;
    final int index;
    Order head;
    Order tail;
    long totalQuantity;
    int orderCount;

    PriceLevel(long price, int index) {
        this.price = price;
        this.index = index;
    }

    void append(Order order) {
        order.level = this;
        order.next = null;
        order.prev = tail;
        if (tail == null) {
            head = order;
        } else {
            tail.next = order;
        }
        tail = order;
        totalQuantity += order.leavesQuantity;
        orderCount++;
    }

    /** Unlinks {@code order}. Its remaining {@code leavesQuantity} is taken off the level total. */
    void remove(Order order) {
        Order prev = order.prev;
        Order next = order.next;
        if (prev == null) {
            head = next;
        } else {
            prev.next = next;
        }
        if (next == null) {
            tail = prev;
        } else {
            next.prev = prev;
        }
        totalQuantity -= order.leavesQuantity;
        orderCount--;
        order.prev = null;
        order.next = null;
        order.level = null;
    }

    boolean isEmpty() {
        return head == null;
    }

    public long price() {
        return price;
    }

    public long totalQuantity() {
        return totalQuantity;
    }

    public int orderCount() {
        return orderCount;
    }

    /** Oldest order at this level, or {@code null}. Follow {@link #nextInQueue(Order)} for the rest. */
    public Order head() {
        return head;
    }

    public static Order nextInQueue(Order order) {
        return order.next;
    }
}
