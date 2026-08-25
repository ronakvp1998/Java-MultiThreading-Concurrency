package InterviewProblems;

/**
 * ============================================================================
 * 1. HEADER & PROBLEM CONTEXT
 * ============================================================================
 * Problem Statement: Bounded-Buffer Producer-Consumer Problem
 *
 * Implement a thread-safe Bounded Buffer (a queue with a fixed maximum capacity)
 * shared between multiple Producer and Consumer threads.
 *
 * - Producers generate data and add it to the buffer. If the buffer is full,
 *   producers must block (wait) until space becomes available.
 * - Consumers remove data from the buffer. If the buffer is empty, consumers
 *   must block (wait) until data becomes available.
 *
 * Constraints:
 * - The buffer has a fixed capacity `N`.
 * - Must be thread-safe (no race conditions, no data corruption).
 * - Must prevent deadlocks and handle spurious wakeups.
 * - The user requested both `wait/notify` and `ReentrantLock` mechanisms.
 *   Note: `wait()`/`notify()` are strictly for intrinsic `synchronized` blocks.
 *   For `ReentrantLock`, we use its modern equivalent: `Condition.await()`
 *   and `Condition.signal()`. Both approaches are demonstrated below.
 *
 * Input/Output Formats:
 * Input: Multiple threads calling `produce()` and `consume()` concurrently.
 * Output: Console logs showing thread-safe, orderly production and consumption
 * without exceeding the buffer capacity or reading from an empty buffer.
 * ============================================================================
 */

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ProducerConsumerMasterclass {

    /**
     * ============================================================================
     * 2.2 PROGRESSIVE IMPLEMENTATION ROADMAP (Non-DP)
     * ============================================================================
     * Phase 1: Optimal Approach - ReentrantLock with Multiple Conditions
     *
     * Intuition:
     * While the traditional `synchronized` keyword works, it only provides a
     * single wait-set per object. If a consumer consumes an item and calls `notifyAll()`,
     * it wakes up ALL waiting threads (both producers AND other consumers).
     * This causes a "thundering herd" problem and wastes CPU cycles.
     *
     * By using a `ReentrantLock` with TWO distinct `Condition` variables (`notFull`
     * and `notEmpty`), we can precisely control thread wakeups. A consumer will ONLY
     * signal waiting producers, and a producer will ONLY signal waiting consumers.
     *
     * Complexity Analysis:
     * - Time Complexity: O(1) for both put and take operations.
     * - Space Complexity: O(N) where N is the capacity of the buffer.
     * ============================================================================
     */
    static class BoundedBufferOptimal<T> {
        private final Queue<T> queue;
        private final int capacity;

        // 1. The Lock
        private final ReentrantLock lock = new ReentrantLock();

        // 2. The Condition variables
        private final Condition notFull = lock.newCondition();
        private final Condition notEmpty = lock.newCondition();

        public BoundedBufferOptimal(int capacity) {
            this.capacity = capacity;
            this.queue = new LinkedList<>();
        }

        public void produce(T item) throws InterruptedException {
            lock.lock(); // Acquire the lock
            try {
                // ALWAYS wait inside a while loop to handle spurious wakeups
                while (queue.size() == capacity) {
                    notFull.await(); // Buffer is full, wait for 'notFull' signal
                }

                queue.add(item);
                System.out.println(Thread.currentThread().getName() + " Produced: " + item + " | Size: " + queue.size());

                // Signal ONE waiting consumer that the buffer is no longer empty
                notEmpty.signal();
            } finally {
                lock.unlock(); // Always unlock in a finally block to prevent deadlocks if an exception occurs
            }
        }

        public T consume() throws InterruptedException {
            lock.lock();
            try {
                // Wait while the buffer is empty
                while (queue.isEmpty()) {
                    notEmpty.await(); // Buffer is empty, wait for 'notEmpty' signal
                }

                T item = queue.poll();
                System.out.println(Thread.currentThread().getName() + " Consumed: " + item + " | Size: " + queue.size());

                // Signal ONE waiting producer that the buffer is no longer full
                notFull.signal();

                return item;
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * ============================================================================
     * Phase 2: Traditional Approach - Synchronized with wait() and notifyAll()
     *
     * Intuition:
     * This is the classic Java 1.0 approach. We use the intrinsic lock (monitor)
     * of the `BoundedBufferTraditional` object itself by using the `synchronized` keyword.
     * Because there is only one wait-set for the object, we must use `notifyAll()`
     * instead of `notify()` to avoid the risk of a producer accidentally waking up
     * another producer instead of a consumer, which could lead to a deadlock.
     *
     * Complexity Analysis:
     * - Time Complexity: O(1) for put and take, but higher overhead due to notifyAll()
     *   waking up unnecessary threads.
     * - Space Complexity: O(N) where N is the capacity.
     * ============================================================================
     */
    static class BoundedBufferTraditional<T> {
        private final Queue<T> queue;
        private final int capacity;

        public BoundedBufferTraditional(int capacity) {
            this.capacity = capacity;
            this.queue = new LinkedList<>();
        }

        // Method is synchronized, meaning thread acquires lock on 'this' instance
        public synchronized void produce(T item) throws InterruptedException {
            while (queue.size() == capacity) {
                wait(); // Releases the lock and puts thread to sleep
            }

            queue.add(item);
            System.out.println(Thread.currentThread().getName() + " Produced (Trad): " + item + " | Size: " + queue.size());

            // Wakes up ALL waiting threads (Producers and Consumers)
            notifyAll();
        }

        public synchronized T consume() throws InterruptedException {
            while (queue.isEmpty()) {
                wait();
            }

            T item = queue.poll();
            System.out.println(Thread.currentThread().getName() + " Consumed (Trad): " + item + " | Size: " + queue.size());

            notifyAll();

            return item;
        }
    }

    /**
     * ============================================================================
     * 4. TESTING SUITE
     * ============================================================================
     */
    public static void main(String[] args) throws InterruptedException {
        System.out.println("--- Starting Phase 1: ReentrantLock Test ---");
        BoundedBufferOptimal<Integer> optimalBuffer = new BoundedBufferOptimal<>(3);

        // Create 2 Producers and 2 Consumers for the Optimal Buffer
        Thread p1 = new Thread(new ProducerTask(optimalBuffer), "Producer-1");
        Thread p2 = new Thread(new ProducerTask(optimalBuffer), "Producer-2");
        Thread c1 = new Thread(new ConsumerTask(optimalBuffer), "Consumer-1");
        Thread c2 = new Thread(new ConsumerTask(optimalBuffer), "Consumer-2");

        p1.start(); p2.start();
        c1.start(); c2.start();

        // Let it run for a bit, then stop
        Thread.sleep(100);
        p1.interrupt(); p2.interrupt(); c1.interrupt(); c2.interrupt();

        // A brief pause before testing the traditional approach
        Thread.sleep(50);

        System.out.println("\n--- Starting Phase 2: wait()/notifyAll() Test ---");
        BoundedBufferTraditional<Integer> tradBuffer = new BoundedBufferTraditional<>(3);

        Thread p3 = new Thread(new TradProducerTask(tradBuffer), "Trad-Producer-1");
        Thread c3 = new Thread(new TradConsumerTask(tradBuffer), "Trad-Consumer-1");

        p3.start(); c3.start();

        Thread.sleep(100);
        p3.interrupt(); c3.interrupt();
    }

    // ============================================================================
    // Helper Runnable Classes for Testing
    // ============================================================================

    static class ProducerTask implements Runnable {
        private final BoundedBufferOptimal<Integer> buffer;
        public ProducerTask(BoundedBufferOptimal<Integer> buffer) { this.buffer = buffer; }

        @Override
        public void run() {
            int i = 0;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    buffer.produce(++i);
                    Thread.sleep(10); // Simulate work
                }
            } catch (InterruptedException e) {
                System.out.println(Thread.currentThread().getName() + " stopped.");
            }
        }
    }

    static class ConsumerTask implements Runnable {
        private final BoundedBufferOptimal<Integer> buffer;
        public ConsumerTask(BoundedBufferOptimal<Integer> buffer) { this.buffer = buffer; }

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    buffer.consume();
                    Thread.sleep(15); // Simulate slightly slower consumer
                }
            } catch (InterruptedException e) {
                System.out.println(Thread.currentThread().getName() + " stopped.");
            }
        }
    }

    static class TradProducerTask implements Runnable {
        private final BoundedBufferTraditional<Integer> buffer;
        public TradProducerTask(BoundedBufferTraditional<Integer> buffer) { this.buffer = buffer; }

        @Override
        public void run() {
            int i = 0;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    buffer.produce(++i);
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                System.out.println(Thread.currentThread().getName() + " stopped.");
            }
        }
    }

    static class TradConsumerTask implements Runnable {
        private final BoundedBufferTraditional<Integer> buffer;
        public TradConsumerTask(BoundedBufferTraditional<Integer> buffer) { this.buffer = buffer; }

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    buffer.consume();
                    Thread.sleep(15);
                }
            } catch (InterruptedException e) {
                System.out.println(Thread.currentThread().getName() + " stopped.");
            }
        }
    }
}