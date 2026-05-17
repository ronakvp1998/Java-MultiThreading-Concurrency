package Topics.LocksAndConditions.StampedLock.Optimisic;

/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Safely read a multi-variable invariant (e.g., Bid and Ask prices of an asset)
 * in a massively concurrent system without blocking the writer threads. Acquiring
 * a structural lock (like ReentrantReadWriteLock) for every read operation causes
 * cache-line contention on the CPU and severely degrades throughput. We need a
 * mechanism to read data optimistically and detect if a writer interfered mid-read.
 *
 * Real-World Use Case:
 * High-Frequency Trading (HFT) Order Books. The matching engine updates the
 * Bid and Ask prices millions of times per second. Simultaneously, thousands
 * of algorithmic trading bots are reading these prices to calculate the "Spread".
 * Readers must not slow down the critical path of the matching engine.
 *
 * Concurrency Constraints:
 * - Torn Reads: If a reader reads the Bid, gets preempted by the OS, a writer
 *   updates both Bid and Ask, and the reader wakes up to read the Ask, the reader
 *   has captured a pair of prices that never existed simultaneously in the market.
 * - Non-Blocking: The optimistic read must not update any shared synchronization
 *   state (like an AQS queue) to maintain nano-second latencies.
 */

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.StampedLock;

public class HFTOptimisticLocking {

    /**
     * Shared Resource representing an HFT Order Book.
     */
    static class OrderBook {
        private double bidPrice = 100.0;
        private double askPrice = 101.0;

        private final StampedLock lock = new StampedLock();

        /**
         * PRODUCER: The Matching Engine. Strictly requires a pessimistic Write Lock.
         */
        public void updatePrices(double newBid, double newAsk) {
            long stamp = lock.writeLock(); // Exclusive hardware-level lock
            try {
                this.bidPrice = newBid;
                // Simulating a tiny hardware delay that makes torn reads possible
                simulateMicroPause();
                this.askPrice = newAsk;
            } finally {
                // CRITICAL: Unlocking increments the internal version stamp,
                // which is how optimistic readers detect interference.
                lock.unlockWrite(stamp);
            }
        }

        /**
         * CONSUMER: The Trading Bot. Implements the true Optimistic Read Pattern.
         */
        public double calculateSpread() {
            // 1. Obtain an optimistic stamp. THIS DOES NOT BLOCK WRITERS.
            // It merely reads a volatile long from the lock's internal state.
            long stamp = lock.tryOptimisticRead();

            // 2. COPY TO LOCAL THREAD STACK IMMEDIATELY.
            // We must copy the variables because they might be actively mutating.
            double currentBid = this.bidPrice;
            simulateMicroPause(); // Increases the chance of simulating a torn read
            double currentAsk = this.askPrice;

            // 3. VALIDATE: Did the lock's version stamp change since we acquired it?
            if (!lock.validate(stamp)) {
                // 4. FALLBACK: A writer intervened. The data in currentBid/currentAsk
                // is corrupted (torn). We discard it and acquire a pessimistic lock.
                System.err.println("[" + Thread.currentThread().getName() + "] Torn read detected! Falling back to Pessimistic Lock.");

                stamp = lock.readLock(); // Blocks if the matching engine is writing
                try {
                    // Safely re-copy the variables under lock protection
                    currentBid = this.bidPrice;
                    currentAsk = this.askPrice;
                } finally {
                    lock.unlockRead(stamp);
                }
            }

            // 5. CALCULATE & RETURN.
            // Strictly use the local variables, NEVER the class fields, after validation.
            return currentAsk - currentBid;
        }

        private void simulateMicroPause() {
            // Used purely to simulate OS context switching for the demonstration
            try { Thread.sleep(ThreadLocalRandom.current().nextInt(1, 5)); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    /**
     * ============================================================================
     * 2. IMPLEMENTATION & DEMONSTRATION
     * ============================================================================
     */
    public static void main(String[] args) {
        OrderBook orderBook = new OrderBook();

        // SENIOR UPGRADE: Using ExecutorService over raw Threads
        ExecutorService executor = Executors.newFixedThreadPool(4);

        System.out.println("Starting HFT Order Book Simulation...\n");

        // Spawn a Writer Task (The Matching Engine)
        Runnable matchingEngineTask = () -> {
            for (int i = 0; i < 5; i++) {
                double newBid = 100.0 + i;
                double newAsk = 101.5 + i;
                System.out.println(">>> [MatchingEngine] Updating prices to Bid: " + newBid + ", Ask: " + newAsk);
                orderBook.updatePrices(newBid, newAsk);
                try { Thread.sleep(10); } catch (InterruptedException e) {}
            }
        };

        // Spawn Reader Tasks (The Trading Bots)
        Runnable tradingBotTask = () -> {
            for (int i = 0; i < 5; i++) {
                double spread = orderBook.calculateSpread();
                System.out.println("[" + Thread.currentThread().getName() + "] Calculated Spread: " + String.format("%.2f", spread));
                try { Thread.sleep(5); } catch (InterruptedException e) {}
            }
        };

        // Execute asynchronously
        CompletableFuture<Void> writerFuture = CompletableFuture.runAsync(matchingEngineTask, executor);
        CompletableFuture<Void> reader1Future = CompletableFuture.runAsync(tradingBotTask, executor);
        CompletableFuture<Void> reader2Future = CompletableFuture.runAsync(tradingBotTask, executor);

        // Wait for all tasks to complete
        CompletableFuture.allOf(writerFuture, reader1Future, reader2Future).join();

        executor.shutdown();
        System.out.println("\nSimulation Complete.");

        // ============================================================================
        // 3. IN-CODE DEEP DIVE
        // ============================================================================
        /*
         * WHY STAMPED LOCK OPTIMISTIC OVER REENTRANT-READ-WRITE-LOCK?
         * A `ReentrantReadWriteLock` requires readers to write to the underlying
         * AbstractQueuedSynchronizer (AQS) to declare they hold a read lock. Writing
         * to shared memory causes CPU cache invalidation across cores (cache line bouncing).
         * `tryOptimisticRead()` is a pure read operation. It scales infinitely with the
         * number of cores.
         *
         * HAPPENS-BEFORE RELATIONSHIPS & MEMORY BARRIERS:
         * - `tryOptimisticRead()` DOES NOT establish a Happens-Before relationship.
         * - The magic is in `validate(stamp)`. When this returns true, the JVM issues a
         *   LoadLoad/LoadStore memory fence. This mathematically guarantees that the local
         *   variables you just copied represent a consistent snapshot of main memory,
         *   free from compiler reordering or stale CPU cache reads.
         *
         * PITFALLS TO DISCUSS IN INTERVIEWS:
         * 1. Mutating State: Never mutate state based on an optimistic read without
         *    first acquiring a pessimistic write lock.
         * 2. Calculation before Validation: A common mistake is executing business logic
         *    before calling `validate()`. If the read is torn, executing logic on corrupted
         *    data can throw `NullPointerException`s, `ArithmeticException`s, or worse.
         */
    }
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * SCENARIO A: Uncontended (The Fast Path)
 * 1. [TradingBot] Calls `tryOptimisticRead()`. Receives stamp (e.g., 256).
 * 2. [TradingBot] Copies `bidPrice` and `askPrice` to its local thread stack.
 * 3. [TradingBot] Calls `validate(256)`. Since the Matching Engine hasn't acquired
 *    a write lock, it returns TRUE.
 * 4. [TradingBot] Calculates spread and returns. Zero locking overhead was incurred.
 *
 * SCENARIO B: Contended (Torn Read & Fallback)
 * 1. [TradingBot] Calls `tryOptimisticRead()`. Receives stamp 256.
 * 2. [TradingBot] Copies `bidPrice` (e.g., 100.0) to local stack.
 * 3. [MatchingEngine] Acquires `writeLock()`. Mutates `bidPrice` to 101.0 and `askPrice`
 *    to 102.5. Calls `unlockWrite()`. The internal lock stamp increments to 258.
 * 4. [TradingBot] Copies `askPrice` (e.g., 102.5) to local stack.
 *    (At this moment, the bot has a torn read: Bid=100.0, Ask=102.5. Spread = 2.5).
 * 5. [TradingBot] Calls `validate(256)`. Returns FALSE because the lock state is now 258.
 * 6. [TradingBot] Discards the torn data. Enters fallback block. Calls `readLock()`.
 * 7. [TradingBot] Re-reads `bidPrice` (101.0) and `askPrice` (102.5) safely under the lock.
 * 8. [TradingBot] Unlocks and returns the correct spread (1.5).
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: O(1) with nanosecond-level overhead for successful validation.
 *   If validation fails, time complexity degrades to the time required to wait for
 *   and acquire a pessimistic `readLock`.
 * - Space Complexity: O(1) primitive allocation on the thread stack.
 * - Performance Overhead: This is the single most efficient locking mechanism in
 *   Java for read-heavy invariants. It performs identically to reading raw `volatile`
 *   variables when uncontended.
 */