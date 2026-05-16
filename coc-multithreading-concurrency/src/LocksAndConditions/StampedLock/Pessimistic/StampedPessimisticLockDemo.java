package LocksAndConditions.StampedLock.Pessimistic;

/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Coordinate highly concurrent access to shared state using Java 8's `StampedLock`
 * in its Pessimistic modes (Read and Write). `ReentrantReadWriteLock` tracks lock
 * ownership and reentrancy per-thread via the AbstractQueuedSynchronizer (AQS),
 * which creates significant memory and CPU overhead. We need a faster, lighter
 * lock for systems that do not require reentrancy.
 *
 * Real-World Use Case:
 * High-Frequency Trading (HFT) Order Books or real-time gaming state engines.
 * In these systems, methods are small, flat, and extremely fast. We don't need
 * the lock to remember if a thread already holds it (reentrancy); we just need
 * raw throughput.
 *
 * Concurrency Constraints:
 * - Non-Reentrancy: A thread holding a `writeLock` will self-deadlock if it
 *   attempts to acquire another `writeLock` or `readLock`.
 * - Lock Upgrading: `StampedLock` allows safely converting a pessimistic read
 *   lock into a write lock without releasing it entirely, preventing race
 *   conditions during the upgrade.
 */

import java.util.concurrent.locks.StampedLock;

public class StampedPessimisticLockDemo {

    /**
     * Shared Resource representing a Stock Ticker.
     */
    static class MarketTicker {
        private double price = 0.0;
        private int volume = 0;

        private final StampedLock lock = new StampedLock();

        /**
         * PRODUCER: Pessimistic Write Lock.
         * Blocks all other readers and writers.
         */
        public void updateTicker(double newPrice, int addedVolume) {
            // 1. Acquire exclusive write lock. Returns a primitive long stamp.
            long stamp = lock.writeLock();
            try {
                System.out.println(">>> [" + Thread.currentThread().getName() + "] WRITE Lock Acquired. Updating Ticker...");

                this.price = newPrice;
                this.volume += addedVolume;

                // Simulate fast I/O or calculation
                simulateWork(200);
                System.out.println(">>> [" + Thread.currentThread().getName() + "] Ticker Updated -> Price: $" + this.price + ", Vol: " + this.volume);
            } finally {
                // CRITICAL: Must unlock using the exact stamp in a finally block.
                lock.unlockWrite(stamp);
            }
        }

        /**
         * CONSUMER: Pessimistic Read Lock.
         * Blocks writers, but allows multiple concurrent readers.
         */
        public void readTicker() {
            // 1. Acquire non-exclusive read lock. Blocks if a write is in progress.
            long stamp = lock.readLock();
            try {
                System.out.println("[" + Thread.currentThread().getName() + "] READ Lock Acquired. Reading data...");

                // Local copy for safe usage
                double currentPrice = this.price;
                int currentVolume = this.volume;

                simulateWork(100);
                System.out.println("[" + Thread.currentThread().getName() + "] Read Result -> Price: $" + currentPrice + ", Vol: " + currentVolume);
            } finally {
                // CRITICAL: Release read lock using the exact stamp.
                lock.unlockRead(stamp);
            }
        }

        /**
         * ADVANCED PATTERN: Conditional Lock Upgrading.
         * Very common in Senior interviews.
         */
        public void conditionalPriceReset(double threshold) {
            long stamp = lock.readLock();
            try {
                // If the price hasn't hit the threshold, we just read and leave.
                while (this.price >= threshold) {
                    // We need to mutate state. Attempt to upgrade to a Write Lock WITHOUT
                    // dropping our current Read Lock.
                    long writeStamp = lock.tryConvertToWriteLock(stamp);

                    if (writeStamp != 0L) {
                        // SUCCESS: We upgraded to a write lock atomically!
                        stamp = writeStamp;
                        System.out.println("!!! [" + Thread.currentThread().getName() + "] Lock UPGRADED to Write. Resetting Ticker.");
                        this.price = 0.0;
                        this.volume = 0;
                        break; // Work is done, exit the loop
                    } else {
                        // FAILED: Another thread holds a read lock, preventing the upgrade.
                        // We must manually drop our read lock and queue for a strict write lock.
                        lock.unlockRead(stamp);
                        stamp = lock.writeLock(); // Blocks until available
                        // The loop restarts to re-evaluate `this.price >= threshold`
                        // because another thread might have changed it while we waited!
                    }
                }
            } finally {
                // StampedLock's unlock() method is smart enough to know if the stamp
                // currently represents a read lock or a write lock.
                lock.unlock(stamp);
            }
        }

        private void simulateWork(long millis) {
            try { Thread.sleep(millis); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    /**
     * ============================================================================
     * 2. IMPLEMENTATION & DEMONSTRATION
     * ============================================================================
     */
    public static void main(String[] args) throws InterruptedException {
        MarketTicker ticker = new MarketTicker();

        // Start with an initial update
        ticker.updateTicker(150.00, 1000);

        // Spawn concurrent readers
        Thread reader1 = new Thread(ticker::readTicker, "Reader-1");
        Thread reader2 = new Thread(ticker::readTicker, "Reader-2");

        // Spawn a concurrent writer
        Thread writer1 = new Thread(() -> ticker.updateTicker(155.50, 500), "Writer-1");

        // Spawn the conditional upgrader
        Thread upgrader = new Thread(() -> ticker.conditionalPriceReset(155.00), "Upgrader-Thread");

        reader1.start();
        reader2.start();
        writer1.start();

        Thread.sleep(500); // Give writer time to push price above threshold
        upgrader.start();

        // ============================================================================
        // 3. IN-CODE DEEP DIVE
        // ============================================================================
        /*
         * WHY STAMPED LOCK PESSIMISTIC OVER REENTRANT-READ-WRITE-LOCK?
         * 1. Throughput: By dropping the requirement to track reentrancy (which thread
         *    owns what lock how many times), `StampedLock` executes faster at the CPU level.
         * 2. Upgrading: `ReentrantReadWriteLock` STRICTLY forbids upgrading a read lock
         *    to a write lock (it will instantly deadlock). `StampedLock.tryConvertToWriteLock()`
         *    allows this, which is a massive architectural advantage for "read-mostly,
         *    sometimes-write" caches.
         *
         * HAPPENS-BEFORE RELATIONSHIPS:
         * A call to `lock.unlockWrite(stamp)` Happens-Before any subsequent successful
         * call to `lock.readLock()` or `lock.writeLock()`. Memory is fully fenced and
         * flushed. `price` and `volume` do not need to be `volatile`.
         *
         * PITFALLS TO DISCUSS IN INTERVIEWS:
         * 1. Self-Deadlock: Calling `writeLock()` from inside a method that already called
         *    `readLock()` (without using the convert API) will freeze the thread permanently.
         * 2. Stamp Forgery/Loss: The lock state is entirely managed by the `long` stamp.
         *    If you accidentally overwrite the variable holding the stamp, or pass a 0,
         *    calling `unlock()` will throw an `IllegalMonitorStateException`.
         * 3. Interruptibility: Standard `readLock()` and `writeLock()` do NOT respond to
         *    Thread interrupts. If you need interruptibility, you must use `readLockInterruptibly()`.
         */
    }
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * 1. [Main] Ticker initialized. `updateTicker` sets initial state to $150.
 * 2. [Main] Starts Reader-1, Reader-2, and Writer-1 almost simultaneously.
 * 3. [Reader-1 & 2] Successfully acquire `readLock()` because no write is active.
 *    They both read $150 concurrently.
 * 4. [Writer-1] Calls `writeLock()`. It BLOCKS because Reader 1 & 2 hold active read locks.
 * 5. [Reader-1 & 2] Finish reading and call `unlockRead(stamp)`.
 * 6. [Writer-1] Unblocks, acquires the write lock, mutates price to $155.50, and unlocks.
 * 7. [Upgrader] Starts. Acquires `readLock()`. Sees price ($155.50) >= threshold ($155.00).
 * 8. [Upgrader] Calls `tryConvertToWriteLock()`. If no other thread started a read, this
 *    returns a valid write stamp instantly. The thread mutates the state safely and
 *    exits, calling `unlock(stamp)` which drops the write lock.
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: Lock acquisition is O(1).
 * - Space Complexity: O(1) primitive long allocation. No node-allocation overhead
 *   like `ReentrantLock` under low contention.
 * - Performance Overhead: This is the fastest pessimistic lock available in the JDK.
 *   However, because it lacks reentrancy, it pushes the complexity onto the developer.
 *   Code must be perfectly flat and carefully orchestrated.
 */