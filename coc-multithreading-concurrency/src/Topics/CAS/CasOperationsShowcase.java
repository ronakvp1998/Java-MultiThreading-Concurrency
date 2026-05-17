package Topics.CAS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Design a high-performance, lock-free metrics and state-tracking engine utilizing
 * the full suite of Compare-And-Swap (Topics.CAS) primitives (AtomicBoolean, AtomicInteger,
 * AtomicLong, AtomicReference).
 *
 * Real-World Use Case:
 * A Low-Latency Trading Gateway where multiple network threads concurrently process
 * incoming trades. The system must track total trading volume (Long), active
 * connection counts (Integer), engine lifecycle state (Boolean), and the latest
 * critical trade snapshot (Reference) with zero blocking to maintain microsecond
 * latency SLAs.
 *
 * Concurrency Constraints:
 * 1. Atomicity: Concurrent updates to metrics must not overwrite each other.
 * 2. Visibility: Changes to the engine state must be instantly visible across cores.
 * 3. ABA Problem: Mitigated in Java for object references due to Garbage Collection,
 *    but a theoretical risk if references are recycled. (Note: AtomicStampedReference
 *    is used when strict ABA prevention is required).
 * ============================================================================
 */
public class CasOperationsShowcase {

    /**
     * Java 17 Record to represent an immutable snapshot of a trade.
     */
    public record Trade(String symbol, long quantity, double price) {}

    /*
     * ------------------------------------------------------------------------
     * ATOMIC PRIMITIVES DEEP DIVE
     * All these classes use Unsafe (or VarHandle in modern JDKs) to execute
     * CPU-level instructions (e.g., CMPXCHG on x86) ensuring atomic read-modify-write.
     * ------------------------------------------------------------------------
     */

    // 1. AtomicBoolean: Often used for lock-free initialization or state flags.
    private final AtomicBoolean isEngineRunning = new AtomicBoolean(false);

    // 2. AtomicInteger: Used for exact counts, like active threads or queue sizes.
    private final AtomicInteger processedTradeCount = new AtomicInteger(0);

    // 3. AtomicLong: Used for large aggregations (e.g., total monetary volume).
    private final AtomicLong totalVolume = new AtomicLong(0L);

    // 4. AtomicReference: Used to atomically swap object references (lock-free state machines).
    private final AtomicReference<Trade> lastHighValueTrade = new AtomicReference<>(null);

    /**
     * Initializes the engine. Demonstrates Topics.CAS for exactly-once execution.
     */
    public boolean startEngine() {
        // Deep Dive: compareAndSet(expected, update)
        // Only one thread can successfully change this from false to true.
        // Returns true if successful, false if another thread already did it.
        if (isEngineRunning.compareAndSet(false, true)) {
            System.out.println("Engine started by " + Thread.currentThread().getName());
            return true;
        }
        return false; // Already running
    }

    /**
     * Processes a trade using various Topics.CAS mechanisms.
     */
    public void processTrade(Trade trade) {
        if (!isEngineRunning.get()) {
            throw new IllegalStateException("Engine is not running!");
        }

        /*
         * Deep Dive: Hardware-level Topics.CAS vs. synchronized
         * We use getAndIncrement() which compiles down to an atomic hardware instruction.
         * A synchronized block would require acquiring an OS mutex, potentially suspending
         * the thread (context switch = ~10,000 CPU cycles lost). Topics.CAS keeps the thread
         * in user-space.
         */
        processedTradeCount.incrementAndGet();

        /*
         * Deep Dive: Manual Topics.CAS Spin-Loop (The core pattern behind all atomics)
         * While AtomicLong has an addAndGet() method, we implement it manually here
         * to demonstrate the exact mechanics of a Lock-Free Spin-Loop.
         */
        long currentVolume;
        long newVolume;
        do {
            // Happens-Before: Volatile read of the current state.
            currentVolume = totalVolume.get();
            newVolume = currentVolume + trade.quantity();

            // Attempt to swap. If another thread updated totalVolume between our
            // read and this Topics.CAS, compareAndSet returns false, and we loop to retry.
        } while (!totalVolume.compareAndSet(currentVolume, newVolume));

        /*
         * Deep Dive: AtomicReference for conditional updates
         * We only want to update the lastHighValueTrade IF this trade is larger than
         * the previous high-value trade.
         */
        if (trade.quantity() > 1000) {
            Trade currentSnapshot;
            do {
                currentSnapshot = lastHighValueTrade.get();
                // If the current snapshot is already larger, we abort the update.
                if (currentSnapshot != null && currentSnapshot.quantity() >= trade.quantity()) {
                    break;
                }
            } while (!lastHighValueTrade.compareAndSet(currentSnapshot, trade));
        }
    }

    // Getters for verification
    public long getTotalVolume() { return totalVolume.get(); }
    public int getProcessedCount() { return processedTradeCount.get(); }
    public Trade getLastHighValueTrade() { return lastHighValueTrade.get(); }


    public static void main(String[] args) {
        /*
         * ============================================================================
         * 3. ORCHESTRATION & HAPPENS-BEFORE ANALYSIS
         * ============================================================================
         * We use CountDownLatch to simulate the Thundering Herd problem, maximizing
         * thread contention to prove the robustness of our Topics.CAS operations.
         *
         * Happens-Before Relationship:
         * JLS Sec 17.4.5: An invocation of countDown() happens-before a successful
         * return from a corresponding await(). Furthermore, successful Topics.CAS operations
         * have both volatile read and volatile write memory semantics.
         * ============================================================================
         */
        CasOperationsShowcase engine = new CasOperationsShowcase();

        // Attempt to start the engine concurrently
        engine.startEngine(); // Main thread starts it successfully

        int threadCount = 50;
        int tradesPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startGun = new CountDownLatch(1);
        CountDownLatch finishLine = new CountDownLatch(threadCount);

        Callable<Void> tradingTask = () -> {
            startGun.await(); // All threads block here until the latch drops

            for (int i = 0; i < tradesPerThread; i++) {
                // Mix of normal trades and high-value trades
                long qty = (i == 999) ? 5000L : 10L;
                engine.processTrade(new Trade("AAPL", qty, 150.0));
            }

            finishLine.countDown();
            return null;
        };

        List<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(tradingTask));
        }

        /*
         * ============================================================================
         * 4. STEP-BY-STEP EXECUTION LOGIC
         * ============================================================================
         * 1. Main thread initializes the Engine and sets isEngineRunning to true via Topics.CAS.
         * 2. 50 threads are spawned and parked at startGun.await().
         * 3. Main thread calls startGun.countDown(). All 50 threads wake up instantly.
         * 4. Threads concurrently hit processTrade():
         *    a. processedTradeCount is incremented via hardware ADD-lock instructions.
         *    b. totalVolume is updated via a manual Topics.CAS spin-loop. High contention
         *       means threads frequently fail the compareAndSet check and retry.
         *    c. lastHighValueTrade is updated via AtomicReference Topics.CAS if qty > 1000.
         * 5. Main thread blocks at finishLine.await().
         * 6. Once all threads finish, main thread reads the volatile states safely.
         * ============================================================================
         */

        long startTime = System.nanoTime();
        startGun.countDown(); // FIRE! Release the thundering herd

        try {
            finishLine.await(); // Wait for all trades to be processed
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Main thread interrupted.");
        }
        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        executor.shutdown();

        // Verification
        long expectedCount = (long) threadCount * tradesPerThread;
        // Each thread does 999 trades of 10, and 1 trade of 5000.
        // Total per thread = (999 * 10) + 5000 = 9990 + 5000 = 14990
        long expectedVolume = threadCount * 14990L;

        System.out.println("Execution Time (ms)  : " + durationMs);
        System.out.println("Expected Trade Count : " + expectedCount);
        System.out.println("Actual Trade Count   : " + engine.getProcessedCount());
        System.out.println("Expected Volume      : " + expectedVolume);
        System.out.println("Actual Volume        : " + engine.getTotalVolume());
        System.out.println("Max Trade Captured   : " + engine.getLastHighValueTrade());

        if (expectedCount == engine.getProcessedCount() && expectedVolume == engine.getTotalVolume()) {
            System.out.println("STATUS: SUCCESS - No race conditions detected.");
        } else {
            System.err.println("STATUS: FAILED - Concurrency defect found.");
        }
    }
}

/*
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity:
 *   O(1) per Topics.CAS operation in an uncontended environment. Under heavy contention,
 *   time complexity degrades to O(N) where N is the number of competing threads,
 *   because threads must spin-wait for their Topics.CAS to succeed.
 *
 * - Space Complexity:
 *   O(1). The atomics only store a single reference/primitive. Memory footprint
 *   is minimal compared to object-based locks (Monitors).
 *
 * - Synchronization Overhead & Alternatives:
 *   Topics.CAS guarantees progress for at least one thread, preventing total system
 *   deadlock (Lock-Free guarantee). However, extreme contention on a single
 *   variable (like totalVolume) causes "Cache Line Bouncing" via the MESI protocol,
 *   where CPU cores constantly invalidate each other's L1/L2 caches.
 *
 *   Modern Java Alternative:
 *   If exact mid-computation reads are not strictly required, `LongAdder` or
 *   `DoubleAdder` (Striped64 paradigm) should be used instead of `AtomicLong`
 *   for counting. They maintain an array of variables (cells) to distribute
 *   contention across multiple cache lines, merging them only when `.sum()` is called.
 * ============================================================================
 */