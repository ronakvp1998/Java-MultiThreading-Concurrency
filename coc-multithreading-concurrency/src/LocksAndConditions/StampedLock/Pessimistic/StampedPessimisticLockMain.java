/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement: 
 * Demonstrate the pessimistic locking capabilities of Java 8's `StampedLock`. 
 * Unlike its optimistic read feature, StampedLock also provides strict 
 * pessimistic read and write locks, similar to `ReentrantReadWriteLock`, 
 * but with different reentrancy and performance characteristics.
 *
 * Real-World Use Case: 
 * Systems that require extremely high throughput and low overhead for locking, 
 * but where operations cannot be done optimistically (e.g., operations involving 
 * side effects like I/O, or complex multi-step state mutations where rollback 
 * is impossible).
 *
 * Concurrency Constraints & Critical Code Analysis: 
 * - Explicit Stamp Management: Locks return a `long` stamp that MUST be used 
 *   to release the lock. Losing or altering this stamp causes fatal state errors.
 * - Non-Reentrant: If a thread holds a write lock and tries to acquire it again, 
 *   it will deadlock.
 * - ⚠️ CRITICAL INTERVIEW PITFALL IN PROVIDED LOGIC: The provided code mutates 
 *   shared state (`isAvailable = true`) inside a `readLock`. Because multiple 
 *   threads can hold a `readLock` simultaneously (like thread1 and thread2), 
 *   this creates a severe Data Race. In a Tier-1 interview, you must always 
 *   use a `writeLock` for state mutation. As requested, the logic remains untouched, 
 *   but this anti-pattern is documented in the deep dive below.
 */

package LocksAndConditions.StampedLock.Pessimistic;

import java.util.concurrent.locks.StampedLock;

public class StampedPessimisticLockMain {
    public static void main(String[] args) {
        PessimisticSharedResource resource = new PessimisticSharedResource();

        // Threads 1 and 2 act as concurrent producers
        Thread thread1 = new Thread(() -> resource.producer(), "Producer-1");
        Thread thread2 = new Thread(() -> resource.producer(), "Producer-2");

        // Thread 3 acts as the consumer
        Thread thread3 = new Thread(() -> resource.consume(), "Consumer-1");

        thread1.start();
        thread2.start();
        thread3.start();
    }
}

class PessimisticSharedResource {

    boolean isAvailable = false;
    StampedLock lock = new StampedLock();

    public void producer() {
        // 1. Acquire a pessimistic read lock. Blocks if a write lock is currently held.
        long stamp = lock.readLock();
        try {
            System.out.println("Read lock acquired by: " + Thread.currentThread().getName());

            // ========================================================================
            // ⚠️ SENIOR ENGINEER DEEP DIVE: THE "MUTATING READER" ANTI-PATTERN
            // ========================================================================
            // In the JVM, `readLock()` is non-exclusive. This means Producer-1 and 
            // Producer-2 can both execute the line below at the exact same nanosecond.
            // Mutating state (`isAvailable = true`) without an exclusive lock creates 
            // a data race. If this were a counter (e.g., count++), updates would be lost.
            // For true thread safety, mutations MUST occur under a `writeLock()`.
            isAvailable = true;

            // Simulating work. Because both Producer threads can hold the read lock 
            // simultaneously, they will likely sleep concurrently.
            Thread.sleep(3000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 2. Release the read lock using the exact primitive long stamp returned.
            lock.unlock(stamp);
            System.out.println("Read lock release by : " + Thread.currentThread().getName());
        }
    }

    public void consume() {
        // 3. Acquire an exclusive write lock. Blocks until all read locks are released.
        long stamp = lock.writeLock();
        try {
            System.out.println("write lock acquired by : " + Thread.currentThread().getName());

            // Safe mutation. The write lock guarantees exclusive access, so no other 
            // thread can read or write while this executes.
            isAvailable = false;

            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 4. Release the write lock.
            lock.unlock(stamp);
            System.out.println("write lock release by : " + Thread.currentThread().getName());
        }
    }

    // ============================================================================
    // 3. IN-CODE DEEP DIVE (General Architecture)
    // ============================================================================
    /*
     * WHY STAMPED LOCK INSTEAD OF REENTRANT-READ-WRITE-LOCK?
     * `StampedLock` is significantly faster than `ReentrantReadWriteLock` in highly
     * concurrent environments because it uses a simpler, non-reentrant internal state
     * mechanism. It avoids the complex bookkeeping required to track which thread holds
     * which lock.
     *
     * HAPPENS-BEFORE RELATIONSHIPS:
     * - Write Lock: `lock.unlock(stamp)` on a write lock Happens-Before any subsequent
     *   successful `readLock()` or `writeLock()`. Memory is safely flushed.
     * - Read Lock: Multiple concurrent readers have NO Happens-Before relationship with
     *   each other. This is why the `producer()` logic above is technically unsafe.
     *
     * EDGE CASES / PITFALLS:
     * 1. Stamp Forgery/Loss: `unlock(stamp)` will throw an `IllegalMonitorStateException`
     *    if the stamp is 0 or doesn't match the current lock state.
     * 2. Reentrancy Deadlock: If `consume()` called another method that also requested
     *    `lock.writeLock()`, the thread would deadlock itself instantly.
     */
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * Based on the strict logic provided:
 * 1. [Main] Starts Producer-1, Producer-2, and Consumer-1.
 * 2. [Producer-1 & Producer-2] Both request `readLock()`. Because no write lock
 *    is active, BOTH instantly acquire the read lock simultaneously.
 * 3. [Consumer-1] Requests `writeLock()`. Because read locks are actively held by
 *    the producers, Consumer-1 enters a BLOCKED/WAITING state.
 * 4. [Producer-1 & Producer-2] Concurrently execute `isAvailable = true` (Data Race)
 *    and sleep for 3000ms simultaneously.
 * 5. [Producer-1 & Producer-2] Wake up and call `unlock(stamp)` approximately at the
 *    same time.
 * 6. [Consumer-1] Once BOTH read locks are released, Consumer-1 unblocks, successfully
 *    acquires the exclusive `writeLock()`, and mutates `isAvailable = false`.
 * 7. [Consumer-1] Sleeps for 1000ms, wakes up, and unlocks.
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: O(1) for lock acquisition.
 * - Space Complexity: O(1) primitive allocation (just the `long` stamp).
 * - Performance Overhead: StampedLock's pessimistic locks perform better than
 *   `ReentrantReadWriteLock` because they lack the overhead of tracking reentrancy
 *   per thread. However, the lack of reentrancy makes them dangerous in complex,
 *   nested method calls. They are best used for short, simple, encapsulated state
 *   changes.
 */