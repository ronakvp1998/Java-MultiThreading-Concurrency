package LocksAndConditions.ReentrantLock;

/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Coordinate multiple threads accessing a shared resource, but with advanced
 * requirements that a standard `synchronized` block cannot handle—such as
 * bounded wait times (timeouts), fairness (FIFO queueing), and reentrancy
 * (a thread re-acquiring a lock it already holds without deadlocking).
 *
 * Real-World Use Case:
 * High-Frequency Trading (HFT) Order Matching Engines or Distributed Database
 * Connection Pools. In these low-latency systems, a thread cannot afford to
 * block infinitely waiting for a monitor lock. It must attempt to acquire the
 * lock, wait for a strict SLA (e.g., 50ms), and if unsuccessful, abort or
 * route the request elsewhere.
 *
 * Concurrency Constraints:
 * - Deadlock Avoidance: Threads must not block indefinitely.
 * - Thread Starvation: Highly contested locks can starve threads. We need
 *   a mechanism to ensure "fair" distribution of CPU time.
 * - Lock Release Guarantee: The lock must be guaranteed to release even if
 *   the critical section throws a RuntimeException.
 */

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class  ReentrantLockDemonstration {

    /**
     * Shared Resource representing an HFT Order Matching Engine.
     */
    static class OrderMatchingEngine {

        // 1. Reentrancy: Allows a thread to acquire the lock multiple times.
        // 2. Fairness (true): The longest-waiting thread gets the lock next.
        //    (Note: Fairness reduces overall throughput but prevents starvation).
        private final ReentrantLock lock = new ReentrantLock(true);
        private int processedOrders = 0;

        public void processOrder(String orderId) {
            System.out.println("[" + Thread.currentThread().getName() + "] Attempting to acquire lock for " + orderId);

            try {
                // SENIOR UPGRADE:
                // Instead of blocking forever with lock.lock() (like 'synchronized'),
                // we use tryLock to implement a bounded wait.
                if (lock.tryLock(2, TimeUnit.SECONDS)) {
                    try {
                        System.out.println("[" + Thread.currentThread().getName() + "] Lock ACQUIRED. Processing " + orderId);

                        // Simulate business logic
                        Thread.sleep(1000);
                        processedOrders++;

                        // Demonstrate Reentrancy: Calling another method that requires the same lock
                        auditOrder(orderId);

                    } finally {
                        // CRITICAL: ALWAYS put unlock() in a finally block IMMEDIATELY
                        // following the successful lock acquisition block.
                        lock.unlock();
                        System.out.println("[" + Thread.currentThread().getName() + "] Lock RELEASED.");
                    }
                } else {
                    // This block executes if the lock couldn't be acquired within 2 seconds.
                    System.err.println("[" + Thread.currentThread().getName() + "] TIMEOUT: Could not acquire lock for " + orderId + ". Aborting to prevent bottleneck.");
                }
            } catch (InterruptedException e) {
                // tryLock(time) is interruptible! It will throw this if another thread
                // interrupts this one while it is waiting for the lock.
                Thread.currentThread().interrupt();
                System.err.println("[" + Thread.currentThread().getName() + "] Was interrupted while waiting for the lock.");
            }
        }

        private void auditOrder(String orderId) {
            // Re-acquiring the lock we already hold.
            // If this was a non-reentrant lock, this line would cause a self-deadlock!
            lock.lock();
            try {
                System.out.println("    -> [" + Thread.currentThread().getName() + "] Re-acquired lock for Auditing " + orderId);
                // The lock 'hold count' is now 2.
            } finally {
                lock.unlock(); // 'hold count' drops to 1.
            }
        }
    }

    /**
     * ============================================================================
     * 2. IMPLEMENTATION & DEMONSTRATION
     * ============================================================================
     */
    public static void main(String[] args) {
        OrderMatchingEngine engine = new OrderMatchingEngine();

        // Create 3 threads competing for the same lock
        Runnable task = () -> {
            String orderId = "ORD-" + (int)(Math.random() * 1000);
            engine.processOrder(orderId);
        };

        Thread t1 = new Thread(task, "Thread-1");
        Thread t2 = new Thread(task, "Thread-2");
        Thread t3 = new Thread(task, "Thread-3");

        t1.start();
        t2.start();
        t3.start();

        // ============================================================================
        // 3. IN-CODE DEEP DIVE
        // ============================================================================
        /*
         * WHY REENTRANTLOCK OVER SYNCHRONIZED?
         * 1. tryLock(timeout): Prevents infinite blocking. Vital for resilient systems.
         * 2. Interruptibility: lockInterruptibly() allows threads waiting for a lock
         *    to be killed cleanly. 'synchronized' threads cannot be interrupted while waiting.
         * 3. Fairness: `new ReentrantLock(true)` guarantees FIFO ordering. 'synchronized'
         *    is inherently unfair and can cause severe thread starvation.
         * 4. Lock Polling: You can check `lock.isLocked()` or `lock.getQueueLength()`.
         *
         * HAPPENS-BEFORE RELATIONSHIP:
         * According to the JMM (JSR-133), an `unlock()` on a ReentrantLock Happens-Before
         * every subsequent successful `lock()` on that SAME lock. This provides the exact
         * same memory visibility guarantees as entering/exiting a `synchronized` block.
         * Any writes to `processedOrders` made prior to `unlock()` are guaranteed visible
         * to the next thread that acquires the lock.
         *
         * PITFALLS:
         * 1. Unlocking an unheld lock: If `tryLock()` fails, but you still call `unlock()`
         *    in a global `finally` block, it throws `IllegalMonitorStateException`. You MUST
         *    only unlock if acquisition was successful.
         * 2. Fairness Overhead: Fair locks suffer from much lower throughput because the
         *    OS has to context-switch to wake up the *specific* thread at the front of the queue,
         *    rather than just letting a currently running thread grab it.
         */
    }
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * 1. T1, T2, and T3 start almost simultaneously and hit `engine.processOrder()`.
 * 2. T1 arrives first and successfully acquires the lock via `tryLock()`. Hold count = 1.
 * 3. T2 and T3 arrive a microsecond later. The lock is held by T1. Because we used
 *    `tryLock(2, TimeUnit.SECONDS)`, T2 and T3 do NOT block forever. They enter a
 *    WAITING state with a 2-second timeout timer.
 * 4. T1 sleeps for 1 second simulating work.
 * 5. T1 calls `auditOrder()`. It calls `lock.lock()`. Because it already owns the lock,
 *    the JVM increments the hold count to 2. It does not deadlock.
 * 6. T1 unlocks (hold count = 1), then exits the main `finally` block and unlocks
 *    again (hold count = 0). The lock is now free.
 * 7. T1's `unlock()` triggers a JMM Happens-Before flush to main memory.
 * 8. Because the lock is "Fair", the JVM strictly grants the lock to the thread
 *    that has been waiting the longest (e.g., T2).
 * 9. T2 successfully acquires the lock, processes, and releases.
 * 10. If T1 and T2 took longer than 2 seconds combined, T3's timer would expire.
 *     T3 would gracefully abort and print the TIMEOUT warning instead of hanging the system.
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: O(1) for lock acquisition under low contention. Under high
 *   contention with a Fair lock, throughput drops significantly due to queue management.
 * - Space Complexity: O(1) heap allocation (the lock object maintains an internal queue
 *   of waiting thread nodes via AQS - AbstractQueuedSynchronizer).
 * - Performance Overhead: Uncontended ReentrantLock performs nearly identically to
 *   `synchronized` in Java 17+. The overhead comes purely from contention and the
 *   chosen fairness policy.
 */