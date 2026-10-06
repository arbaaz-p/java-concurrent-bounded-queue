package dev.arbaaz.concurrent;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Small dependency-free test runner for the queue's concurrency behaviour. */
public final class BoundedBlockingQueueTest {
    private static int testsPassed;

    private BoundedBlockingQueueTest() {
    }

    public static void main(String[] args) throws Exception {
        testRejectsInvalidCapacity();
        testRejectsNullElements();
        testPreservesFifoOrder();
        testTakeBlocksUntilAnElementArrives();
        testPutBlocksUntilSpaceIsAvailable();
        testMultipleProducersAndConsumers();
        System.out.println("PASS: " + testsPassed + " concurrency tests");
    }

    private static void testRejectsInvalidCapacity() {
        expectThrows(IllegalArgumentException.class, () -> new BoundedBlockingQueue<>(0));
        pass();
    }

    private static void testRejectsNullElements() {
        BoundedBlockingQueue<String> queue = new BoundedBlockingQueue<>(1);
        expectThrows(NullPointerException.class, () -> queue.put(null));
        pass();
    }

    private static void testPreservesFifoOrder() throws Exception {
        BoundedBlockingQueue<Integer> queue = new BoundedBlockingQueue<>(3);
        queue.put(10);
        queue.put(20);
        queue.put(30);
        assertEquals(10, queue.take(), "first element");
        assertEquals(20, queue.take(), "second element");
        assertEquals(30, queue.take(), "third element");
        assertEquals(0, queue.size(), "queue size after draining");
        pass();
    }

    private static void testTakeBlocksUntilAnElementArrives() throws Exception {
        BoundedBlockingQueue<String> queue = new BoundedBlockingQueue<>(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> waitingTake = executor.submit(queue::take);
            assertStillWaiting(waitingTake, "take should wait on an empty queue");
            queue.put("evidence-record");
            assertEquals("evidence-record", waitingTake.get(1, TimeUnit.SECONDS), "released take");
        } finally {
            shutdown(executor);
        }
        pass();
    }

    private static void testPutBlocksUntilSpaceIsAvailable() throws Exception {
        BoundedBlockingQueue<String> queue = new BoundedBlockingQueue<>(1);
        queue.put("first");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> waitingPut = executor.submit(() -> {
                queue.put("second");
                return null;
            });
            assertStillWaiting(waitingPut, "put should wait on a full queue");
            assertEquals("first", queue.take(), "element removed to create space");
            waitingPut.get(1, TimeUnit.SECONDS);
            assertEquals("second", queue.take(), "element added after space was created");
        } finally {
            shutdown(executor);
        }
        pass();
    }

    private static void testMultipleProducersAndConsumers() throws Exception {
        int workers = 4;
        int itemsPerWorker = 250;
        int totalItems = workers * itemsPerWorker;
        BoundedBlockingQueue<Integer> queue = new BoundedBlockingQueue<>(8);
        Set<Integer> consumed = ConcurrentHashMap.newKeySet();
        ExecutorService executor = Executors.newFixedThreadPool(workers * 2);

        try {
            Future<?>[] futures = new Future<?>[workers * 2];
            for (int producer = 0; producer < workers; producer++) {
                final int start = producer * itemsPerWorker;
                futures[producer] = executor.submit(() -> {
                    for (int offset = 0; offset < itemsPerWorker; offset++) {
                        queue.put(start + offset);
                    }
                    return null;
                });
            }
            for (int consumer = 0; consumer < workers; consumer++) {
                futures[workers + consumer] = executor.submit(() -> {
                    for (int i = 0; i < itemsPerWorker; i++) {
                        consumed.add(queue.take());
                    }
                    return null;
                });
            }
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
        } finally {
            shutdown(executor);
        }

        assertEquals(totalItems, consumed.size(), "unique values consumed");
        assertEquals(0, queue.size(), "queue size after concurrent run");
        pass();
    }

    private static void assertStillWaiting(Future<?> future, String message) throws Exception {
        try {
            future.get(120, TimeUnit.MILLISECONDS);
            throw new AssertionError(message);
        } catch (TimeoutException expected) {
            // Timeout is expected because the operation should still be blocked.
        }
    }

    private static void shutdown(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
            throw new AssertionError("executor did not stop");
        }
    }

    private static void pass() {
        testsPassed++;
    }

    private static void assertEquals(Object expected, Object actual, String label) {
        if (!expected.equals(actual)) {
            throw new AssertionError(label + ": expected " + expected + ", got " + actual);
        }
    }

    private static void expectThrows(Class<? extends Throwable> expected, CheckedRunnable action) {
        try {
            action.run();
            throw new AssertionError("expected " + expected.getSimpleName());
        } catch (Throwable actual) {
            if (!expected.isInstance(actual)) {
                throw new AssertionError("expected " + expected.getSimpleName() + ", got " + actual, actual);
            }
        }
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run() throws Exception;
    }
}
