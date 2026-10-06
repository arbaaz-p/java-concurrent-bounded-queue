package dev.arbaaz.concurrent;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A bounded, first-in-first-out queue for coordinating producer and consumer
 * threads. Producers wait when the queue is full and consumers wait when it is
 * empty.
 *
 * @param <T> the type of element stored in the queue
 */
public final class BoundedBlockingQueue<T> {
    private final int capacity;
    private final Deque<T> elements = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    /**
     * Creates an empty queue with a fixed capacity.
     *
     * @param capacity maximum number of elements that may be queued
     */
    public BoundedBlockingQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be greater than zero");
        }
        this.capacity = capacity;
    }

    /**
     * Adds an element, waiting if necessary until space is available.
     *
     * @param element element to add; null values are not permitted
     * @throws InterruptedException if interrupted while waiting or acquiring the lock
     */
    public void put(T element) throws InterruptedException {
        Objects.requireNonNull(element, "element must not be null");
        lock.lockInterruptibly();
        try {
            while (elements.size() == capacity) {
                notFull.await();
            }
            elements.addLast(element);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes and returns the oldest element, waiting if necessary until one
     * becomes available.
     *
     * @return the oldest queued element
     * @throws InterruptedException if interrupted while waiting or acquiring the lock
     */
    public T take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (elements.isEmpty()) {
                notEmpty.await();
            }
            T element = elements.removeFirst();
            notFull.signal();
            return element;
        } finally {
            lock.unlock();
        }
    }

    /** Returns the current number of queued elements. */
    public int size() {
        lock.lock();
        try {
            return elements.size();
        } finally {
            lock.unlock();
        }
    }

    /** Returns the maximum number of elements this queue can hold. */
    public int capacity() {
        return capacity;
    }
}
