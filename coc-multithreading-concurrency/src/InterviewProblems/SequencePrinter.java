package InterviewProblems;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Orchestrate 3 concurrent threads to print numbers from 1 to 10 in strict
 * sequential order. Thread-1 prints 1, Thread-2 prints 2, Thread-3 prints 3,
 * Thread-1 prints 4, and so on.
 *
 * Real-World Use Case:
 * This is a classic "State Machine" or "Turn-Taking" concurrency pattern.
 * In distributed systems or algorithmic trading gateways, network packets or
 * FIX messages may arrive concurrently across multiple I/O threads but must be
 * re-sequenced and processed in strict chronological order by a processing engine
 * before being committed to a ledger.
 *
 * Concurrency Constraints:
 * 1. Mutual Exclusion: Only one thread can evaluate the state and print at a time.
 * 2. Strict Ordering: Threads must not print out of turn.
 * 3. Spurious Wakeups: Threads awoken incorrectly must re-check the condition.
 * 4. Liveness/Deadlock: When the maximum number is reached, all waiting threads
 *    must be safely awoken so they can terminate cleanly without deadlocking.
 * ============================================================================
 */
public class SequencePrinter {

    // Shared state
    private static final int MAX_NUMBERS = 10;
    private static final int TOTAL_THREADS = 3;
    private int currentNumber = 1;

    /*
     * Deep Dive: Why ReentrantLock and Condition?
     * While `synchronized` and `wait()/notifyAll()` could work, ReentrantLock
     * provides superior interruptibility, fairness policies (if needed), and
     * multiple Condition variables. Here, we use a single Condition for simplicity,
     * but the `java.util.concurrent.locks` package is the modern standard for
     * explicit thread signaling.
     */
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition stateChanged = lock.newCondition();

    /**
     * The task executed by each worker thread.
     *
     * @param threadId The logical ID of the thread (1, 2, or 3)
     */
    public void printNumbers(int threadId) {
        while (true) {
            lock.lock(); // Acquire the monitor
            try {
                /*
                 * Happens-Before Relationship:
                 * JLS Sec 17.4.5: The unlocking of a lock happens-before every
                 * subsequent locking of that same lock. This guarantees that
                 * updates to `currentNumber` by one thread are fully visible to
                 * the next thread that acquires the lock.
                 *
                 * Deep Dive: The 'while' loop prevents Spurious Wakeups.
                 * A thread might wake up due to an OS-level signal even if
                 * stateChanged.signalAll() wasn't called. The while loop forces
                 * the thread to re-verify if it is actually its turn.
                 */
                while (currentNumber <= MAX_NUMBERS && currentNumber % TOTAL_THREADS != threadId % TOTAL_THREADS) {
                    stateChanged.await(); // Relinquish lock and sleep
                }

                // Base Case: Exit condition.
                // If we've exceeded MAX_NUMBERS, break the loop to terminate the thread.
                if (currentNumber > MAX_NUMBERS) {
                    // CRITICAL: Wake up any other threads still waiting in await()
                    // so they can evaluate the exit condition and terminate.
                    stateChanged.signalAll();
                    break;
                }

                // Critical Section Execution
                System.out.println("Thread-" + threadId + " printed: " + currentNumber);
                currentNumber++;

                // Signal all waiting threads that the state has changed
                stateChanged.signalAll();

            } catch (InterruptedException e) {
                // Best Practice: Restore the interrupt flag and exit gracefully
                Thread.currentThread().interrupt();
                System.err.println("Thread-" + threadId + " was interrupted.");
                break;
            } finally {
                // Must ALWAYS be in a finally block to prevent deadlocks on exception
                lock.unlock();
            }
        }
    }

    public static void main(String[] args) {
        SequencePrinter printer = new SequencePrinter();

        // Prioritize java.util.concurrent utilities over raw Threads
        ExecutorService executor = Executors.newFixedThreadPool(TOTAL_THREADS);

        /*
         * ============================================================================
         * 4. STEP-BY-STEP EXECUTION LOGIC
         * ============================================================================
         * 1. The Executor submits 3 tasks. Threads start executing `printNumbers`.
         * 2. Suppose Thread-2 acquires the lock first. currentNumber is 1.
         *    Thread-2 checks: `1 <= 10 && 1 % 3 != 2 % 3` (True).
         *    Thread-2 calls `await()`, releasing the lock and sleeping.
         * 3. Thread-1 acquires the lock. currentNumber is 1.
         *    Thread-1 checks: `1 <= 10 && 1 % 3 != 1 % 3` (False).
         *    Thread-1 skips the while loop, prints "1", increments currentNumber to 2,
         *    calls `signalAll()`, and releases the lock.
         * 4. Thread-2 (and Thread-3) wake up and compete for the lock.
         * 5. Thread-2 wins. currentNumber is 2.
         *    Thread-2 checks: `2 <= 10 && 2 % 3 != 2 % 3` (False).
         *    Thread-2 prints "2", increments to 3, signals all, releases lock.
         * 6. This cycle repeats strictly until currentNumber = 11.
         * 7. At 11, whichever thread wakes up sees `currentNumber > MAX_NUMBERS`,
         *    signals others to wake up, and breaks its loop to terminate.
         * ============================================================================
         */

        System.out.println("--- Starting Sequence Printer ---\n");

        // Submit tasks for Thread 1, 2, and 3
        for (int i = 1; i <= TOTAL_THREADS; i++) {
            final int threadId = i;
            executor.submit(() -> printer.printNumbers(threadId));
        }

        // Graceful Orchestration Shutdown
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.println("\n--- Execution Complete ---");
    }
}

/*
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: O(N) where N is MAX_NUMBERS. Each number is evaluated and
 *   printed exactly once.
 *
 * - Space Complexity: O(1). State tracking relies entirely on a few primitive
 *   integers.
 *
 * - Synchronization Overhead:
 *   Using a single `Condition` with `signalAll()` introduces slight overhead
 *   because every time a number is printed, ALL waiting threads are awoken
 *   (Thundering Herd problem), even though only one will pass the while-loop
 *   check.
 *
 *   Optimization for HFT / Extreme Low Latency:
 *   If there were 10+ threads instead of 3, `signalAll()` would waste CPU cycles.
 *   The optimized pattern uses an Array of `Condition` objects (one for each
 *   thread), allowing Thread-1 to specifically call `conditions[1].signal()`
 *   to wake up *only* Thread-2, completely eliminating spurious wakeups and
 *   contention. For 3 threads, the difference is negligible, and single-condition
 *   keeps the code vastly more readable.
 * ============================================================================
 */