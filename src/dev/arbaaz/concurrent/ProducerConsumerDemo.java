package dev.arbaaz.concurrent;

/** Demonstrates two threads exchanging messages through a bounded queue. */
public final class ProducerConsumerDemo {
    private ProducerConsumerDemo() {
    }

    public static void main(String[] args) throws InterruptedException {
        BoundedBlockingQueue<String> queue = new BoundedBlockingQueue<>(3);

        Thread producer = new Thread(() -> {
            try {
                for (int i = 1; i <= 8; i++) {
                    String message = "event-" + i;
                    queue.put(message);
                    System.out.println("produced " + message);
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }, "producer");

        Thread consumer = new Thread(() -> {
            try {
                for (int i = 1; i <= 8; i++) {
                    System.out.println("consumed " + queue.take());
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }, "consumer");

        producer.start();
        consumer.start();
        producer.join();
        consumer.join();
    }
}
