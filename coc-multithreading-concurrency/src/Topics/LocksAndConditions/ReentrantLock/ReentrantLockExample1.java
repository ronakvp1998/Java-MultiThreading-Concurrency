package Topics.LocksAndConditions.ReentrantLock;

/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Demonstrate the fundamental usage of an explicit `ReentrantLock` to protect
 * a critical section, replacing the traditional implicit `synchronized` keyword.
 *
 * Real-World Use Case:
 * Any standard multi-threaded application requiring mutual exclusion. Explicit
 * locks are often preferred in modern codebases because they offer a path to
 * upgrade to advanced features (like try-locking or fairness) without rewriting
 * the entire synchronization architecture.
 *
 * Concurrency Constraints:
 * - Mutual Exclusion: Only one thread can execute the protected code at a time.
 * - Fault Tolerance (Lock Release): If the business logic throws an exception,
 *   the lock MUST still be released to prevent a system-wide deadlock.
 */



import java.util.concurrent.locks.ReentrantLock;

public class ReentrantLockExample1 {

    // Shared state
    boolean isAvailable = false;

    // The explicit lock object. Default is non-fair (higher throughput).
    ReentrantLock lock = new ReentrantLock();

    public void producer() {
        // 1. Acquire the lock BEFORE entering the try block.
        // If lock() throws an exception (rare, usually an Error), we don't want
        // to execute the finally block and attempt to unlock an unheld lock.
        lock.lock();

        try {
            // --- CRITICAL SECTION START ---
            System.out.println("[" + Thread.currentThread().getName() + "] Lock acquired.");

            // Safe state mutation
            isAvailable = true;

            // Simulating business logic or I/O
            Thread.sleep(4000);
            // --- CRITICAL SECTION END ---

        } catch (InterruptedException e) {
            // Best Practice: Always restore the interrupt flag if caught
            Thread.currentThread().interrupt();
            System.err.println("Thread was interrupted.");
        } finally {
            // CRITICAL INTERVIEW POINT:
            // unlock() MUST be in the finally block. This guarantees that even
            // if an exception is thrown, the lock is returned to the OS.
            lock.unlock();
            System.out.println("[" + Thread.currentThread().getName() + "] Lock released.");
        }
    }
}

class ReentrantLockExample1Main {
    public static void main(String[] args) {
        ReentrantLockExample1 resource = new ReentrantLockExample1();

        // Creating two threads that will compete for the exact same lock instance
        Thread thread1 = new Thread(() -> {
            resource.producer();
        }, "Thread-1");

        Thread thread2 = new Thread(() -> {
            resource.producer();
        }, "Thread-2");

        thread1.start();
        thread2.start();

        // ============================================================================
        // 3. IN-CODE DEEP DIVE
        // ============================================================================
        /*
         * WHY THIS APPROACH?
         * This code demonstrates the 1:1 replacement of `synchronized void producer()`.
         * While `synchronized` is simpler to write, `ReentrantLock` delegates blocking
         * to the JVM's internal AbstractQueuedSynchronizer (AQS). AQS uses Topics.CAS
         * (Compare-And-Swap) instructions at the CPU level, making it highly efficient.
         *
         * HAPPENS-BEFORE RELATIONSHIP:
         * ReentrantLock guarantees the exact same memory semantics as `synchronized`.
         * The `lock.unlock()` operation by Thread-1 "Happens-Before" the `lock.lock()`
         * operation by Thread-2. Therefore, when Thread-2 enters the critical section,
         * it is mathematically guaranteed to see `isAvailable = true` written by Thread-1.
         *
         * PITFALLS:
         * 1. Orphaned Locks: Forgetting the `try...finally` block. If `Thread.sleep()`
         *    throws an exception and `unlock()` isn't in a `finally` block, the lock
         *    is permanently held. All other threads will hang forever.
         * 2. Unlocking Unheld Locks: Calling `unlock()` when the current thread doesn't
         *    own the lock throws an `IllegalMonitorStateException`.
         */
    }
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * 1. [Main] Creates `resource`, `Thread-1`, and `Thread-2`.
 * 2. [Main] Starts both threads simultaneously.
 * 3. [T1 & T2] Both threads hit `lock.lock()` at nearly the same CPU cycle.
 * 4. [AQS] The underlying AQS uses an atomic Topics.CAS operation. Let's assume T1 wins.
 * 5. [T1] Acquires the lock. Enters the `try` block, sets `isAvailable = true`,
 *    and begins sleeping for 4 seconds.
 * 6. [T2] Fails the Topics.CAS operation. The AQS puts T2 into a WAITING queue and uses
 *    `LockSupport.park()` to suspend T2 at the OS level (saving CPU cycles).
 * 7. [T1] Wakes up. Exits the `try` block and enters the `finally` block.
 * 8. [T1] Calls `lock.unlock()`. The AQS changes lock ownership to 'free'.
 * 9. [AQS] Notices T2 is in the waiting queue. Uses `LockSupport.unpark()` to wake T2.
 * 10. [T2] Wakes up, automatically acquires the lock, executes its logic, and unlocks.
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: O(1) for acquisition.
 * - Space Complexity: O(1) heap allocation (AQS maintains a lightweight linked list of waiting nodes).
 * - Performance Overhead: Uncontended locks are essentially free (just an atomic integer update).
 *   Under contention, `ReentrantLock` historically outperformed `synchronized` in older JVMs,
 *   though modern Java 17+ has optimized `synchronized` to be nearly identical in speed for basic usage.
 */