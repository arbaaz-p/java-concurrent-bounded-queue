# Java Concurrent Bounded Queue

An expanded CPSC 1181 coursework project exploring the producer-consumer pattern. The original lab used an `ArrayList<String>` with one producer and one consumer. This portfolio version is generic, uses an efficient `ArrayDeque`, validates inputs, supports interruptible lock acquisition, and includes repeatable multithreaded tests.

## What it demonstrates

- `ReentrantLock` protecting shared state
- separate `notEmpty` and `notFull` conditions
- `while` loops around `await()` to handle wakeups safely
- blocking producers and consumers without busy-waiting
- FIFO behaviour with multiple concurrent workers

## Run the tests

Java 17 or newer is recommended.

```bash
mkdir -p out
javac -d out $(find src test -name '*.java')
java -cp out dev.arbaaz.concurrent.BoundedBlockingQueueTest
```

Expected result:

```text
PASS: 6 concurrency tests
```

## Run the demonstration

```bash
java -cp out dev.arbaaz.concurrent.ProducerConsumerDemo
```

## Design notes

`put` waits on `notFull` when the fixed-capacity queue has no space. `take` waits on `notEmpty` when no item is available. Each state change signals one waiting thread on the opposite condition. All state inspection happens while the same lock is held.

This is an educational implementation. Production code should normally use the well-tested queue implementations in `java.util.concurrent`, such as `ArrayBlockingQueue`.
