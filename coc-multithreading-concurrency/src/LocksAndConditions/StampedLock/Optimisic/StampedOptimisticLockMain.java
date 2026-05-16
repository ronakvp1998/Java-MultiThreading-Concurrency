/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement: 
 * Safely implement an Optimistic Read using `StampedLock`. The previous
 * implementation contained a critical anti-pattern (mutating shared state
 * during an optimistic read). This code demonstrates the correct, thread-safe
 * pattern: copying shared state to the local thread stack, validating the
 * stamp, and falling back to a pessimistic lock if a concurrent write occurred.
 *
 * Real-World Use Case:
 * Ultra-low latency systems with a 99.9% Read / 0.1% Write ratio. For example,
 * reading global configuration flags or checking a circuit breaker status where
 * acquiring a structural lock for every read would destroy CPU throughput.
 *
 * Concurrency Constraints & Critical Code Analysis:
 * - Zero Mutation: Optimistic reads MUST NOT mutate shared state. They are
 *   strictly for reading snapshots of data.
 * - Local Stack Copy: Shared variables must be copied to local method variables
 *   BEFORE calling `validate()`.
 * - Fallback Mechanism: If `validate()` returns false, the data read might be
 *   corrupt (torn read). The thread must discard the local data, acquire a
 *   pessimistic `readLock()`, and re-read the data.
 */

package LocksAndConditions.StampedLock.Optimisic;

import java.util.concurrent.locks.StampedLock;

public class StampedOptimisticLockMain {
    public static void main(String[] args) throws InterruptedException {
        SafeOptimisticResource resource = new SafeOptimisticResource();

        // Thread 1: The Optimistic Reader
        Thread reader = new Thread(() -> resource.readData(), "Thread-1-Reader");

        // Thread 2: The Pessimistic Writer
        Thread writer = new Thread(() -> resource.writeData(99), "Thread-2-Writer");

        reader.start();

        // Ensure reader gets the optimistic stamp first before the writer mutates
        Thread.sleep(100);

        writer.start();
    }
}

class SafeOptimisticResource {

    private int a = 10;
    private final StampedLock lock = new StampedLock();

    /**
     * CONSUMER (Reader): The correct Optimistic Read Pattern.
     */
    public void readData() {
        // 1. Obtain an optimistic stamp. THIS DOES NOT BLOCK WRITERS.
        long stamp = lock.tryOptimisticRead();
        System.out.println("[" + Thread.currentThread().getName() + "] Optimistic stamp acquired.");

        // 2. COPY TO LOCAL THREAD STACK IMMEDIATELY.
        int localA = this.a;

        // Simulating processing delay. During this time, the Writer thread
        // will wake up, acquire the write lock, and mutate 'a'.
        simulateWork(2000);

        // 3. VALIDATE: Did any thread acquire a writeLock while we were working?
        if (!lock.validate(stamp)) {
            // 4. FALLBACK: The stamp is invalid. A writer intervened.
            System.err.println("[" + Thread.currentThread().getName() + "] Validation FAILED! Data changed. Falling back to Pessimistic Lock.");

            // Discard the dirty 'localA' and acquire a strict read lock.
            stamp = lock.readLock();
            try {
                // Safely re-read the variable now that we hold a lock
                localA = this.a;
            } finally {
                lock.unlockRead(stamp);
            }
        } else {
            System.out.println("[" + Thread.currentThread().getName() + "] Validation SUCCESS. No contention.");
        }

        // 5. USE THE LOCAL VARIABLE SAFELY.
        System.out.println("[" + Thread.currentThread().getName() + "] Final safely read value of 'a' is: " + localA);
    }

    /**
     * PRODUCER (Writer): Always requires an exclusive write lock.
     */
    public void writeData(int newValue) {
        long stamp = lock.writeLock();
        System.out.println(">>> [" + Thread.currentThread().getName() + "] Write lock acquired.");
        try {
            System.out.println(">>> [" + Thread.currentThread().getName() + "] Mutating state from " + this.a + " to " + newValue);
            this.a = newValue;
        } finally {
            lock.unlockWrite(stamp);
            System.out.println(">>> [" + Thread.currentThread().getName() + "] Write lock released.");
        }
    }

    private void simulateWork(long millis) {
        try { Thread.sleep(millis); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    // ============================================================================
    // 3. IN-CODE DEEP DIVE (General Architecture)
    // ============================================================================
    /*
     * THE FIX EXPLAINED (For Tier-1 Interviews):
     * The original code attempted to mutate shared state (`a = 11`) while holding
     * an optimistic read stamp. Optimistic stamps are not locks; they do not prevent
     * other threads from accessing memory. That caused a race condition.
     *
     * By changing the logic to read into a local variable (`int localA = this.a`),
     * we protect the thread's execution context. If validation fails, we simply throw
     * away `localA` and try again using a safe, pessimistic `readLock()`. We never
     * accidentally overwrite the Writer's data.
     *
     * HAPPENS-BEFORE RELATIONSHIPS & MEMORY BARRIERS:
     * - `validate(stamp)` acts as a LoadLoad/LoadStore memory fence. If it returns true,
     *   the JMM guarantees that the local variable `localA` is exactly as it was when
     *   the stamp was issued, and no writer has interfered.
     */
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * 1. [Main] Starts Thread-1 (Reader).
 * 2. [Thread-1] Calls `tryOptimisticRead()`. Receives a valid stamp.
 * 3. [Thread-1] Copies shared `a` (10) into `localA`. Enters a 2000ms sleep.
 * 4. [Main] Waits 100ms, then starts Thread-2 (Writer).
 * 5. [Thread-2] Calls `writeLock()`. Because an optimistic read does not block,
 *    Thread-2 successfully acquires the lock. The internal lock version increments.
 * 6. [Thread-2] Overwrites the shared variable `a = 99`.
 * 7. [Thread-2] Calls `unlockWrite()`. The internal lock version increments again.
 * 8. [Thread-1] Wakes up. Calls `validate(stamp)`.
 * 9. [Thread-1] Validation FAILS because Thread-2 altered the lock state during the sleep.
 * 10. [Thread-1] Executes the fallback block, acquiring a pessimistic `readLock()`.
 * 11. [Thread-1] Re-reads `this.a` (which is now 99) into `localA`, unlocks, and prints 99.
 *     (Result: The system remains perfectly consistent. No data was corrupted or lost).
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: O(1) for lock acquisition and validation.
 * - Space Complexity: O(1) primitive allocation (`long` stamp and `int` local copy).
 * - Performance Overhead: `tryOptimisticRead` is extremely fast. However, if writes
 *   are frequent, `validate()` will consistently fail, forcing the reader to constantly
 *   acquire pessimistic locks. StampedLock optimistic reads should only be used in
 *   systems heavily skewed towards read operations.
 */