package Topics.LocksAndConditions.SemaphoreLock;

/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Restrict concurrent access to a shared resource or a set of resources to a
 * specific, bounded maximum number of threads. Unlike standard locks which enforce
 * strict mutual exclusion (1 thread at a time), we need a mechanism that allows
 * exactly N threads to enter the critical section simultaneously.
 *
 * Real-World Use Case:
 * - Database Connection Pools: Limiting the system to only 50 concurrent DB connections
 *   to prevent overwhelming the database server.
 * - Rate Limiting / API Throttling: Bounding the number of concurrent outbound HTTP
 *   requests to a third-party API to avoid being IP-banned.
 * - Bounded Queues/Buffers: Regulating Producer-Consumer patterns.
 *
 * Concurrency Constraints:
 * - Resource Exhaustion Prevention: Threads beyond the allowed threshold must block
 *   until a permit becomes available.
 * - Permit Leakage: If a thread crashes inside the critical section, it must
 *   guarantee the permit is returned, otherwise the pool permanently shrinks.
 */


import java.util.concurrent.Semaphore;

public class SemaphoreLockDemo {
    public static void main(String[] args) {
        SemaphoreSharedResource resource = new SemaphoreSharedResource();

        // Spawning 5 threads that will compete for only 2 available permits
        Thread thread1 = new Thread(() -> resource.producer(), "Thread-1");
        Thread thread2 = new Thread(() -> resource.producer(), "Thread-2");
        Thread thread3 = new Thread(() -> resource.producer(), "Thread-3");
        Thread thread4 = new Thread(() -> resource.producer(), "Thread-4");
        Thread thread5 = new Thread(() -> resource.producer(), "Thread-5");

        thread1.start();
        thread2.start();
        thread3.start();
        thread4.start();
        thread5.start();
    }
}

class SemaphoreSharedResource {

    // Shared state
    boolean isAvailable = false;

    // 1. Initialize Semaphore with 2 permits.
    // This allows exactly TWO threads to execute the critical section concurrently.
    Semaphore lock = new Semaphore(2);

    public void producer() {
        try {
            // 2. Request a permit. If 2 threads already hold permits, this will BLOCK
            // the current thread until another thread calls release().
            lock.acquire();
            System.out.println("Lock acquired by : " + Thread.currentThread().getName());

            // ========================================================================
            // ⚠️ SENIOR ENGINEER INTERVIEW NOTE: DATA RACE AWARENESS
            // ========================================================================
            // Because the Semaphore permits 2 threads to be here simultaneously,
            // mutating shared, non-atomic state (`isAvailable = true`) creates a
            // theoretical Data Race. In a real-world scenario, Semaphores are usually
            // used to hand out *independent* resources (like 2 separate DB connections),
            // rather than allowing multiple threads to mutate the same variable concurrently.
            // As per instructions, the logic remains unchanged, but this is a critical
            // distinction to mention in a systems design interview.
            isAvailable = true;

            // Simulating a long-running task (e.g., executing a slow DB query)
            Thread.sleep(4000);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 3. CRITICAL: ALWAYS release the permit in a finally block.
            // If the thread throws an exception, the permit is returned to the pool.
            lock.release();
            System.out.println("lock release by : " + Thread.currentThread().getName());
        }
    }

    // ============================================================================
    // 3. IN-CODE DEEP DIVE (General Architecture)
    // ============================================================================
    /*
     * WHY SEMAPHORE OVER REENTRANT-LOCK?
     * A `ReentrantLock` provides Mutual Exclusion (Mutex) - meaning the permit count
     * is strictly 1. A `Semaphore` is a generalized synchronization primitive that
     * manages a set of abstract "permits". It does not track "ownership" of the lock,
     * it merely tracks the count.
     *
     * HAPPENS-BEFORE RELATIONSHIPS:
     * According to the Java Memory Model (JMM), a successful call to `release()` on a
     * Semaphore Happens-Before any subsequent successful call to `acquire()` on that
     * same Semaphore. This means if Thread-1 updates a shared resource and releases,
     * Thread-3 (which acquires that freed permit) is guaranteed to see Thread-1's updates.
     *
     * EDGE CASES / PITFALLS:
     * 1. Rogue Releases: Because `Semaphore` does not track thread ownership, ANY thread
     *    can call `lock.release()`, even if it never called `acquire()`. This can artificially
     *    inflate the permit count beyond the initial value (e.g., from 2 to 3), completely
     *    breaking the bounds limit.
     * 2. Unfairness: By default, `new Semaphore(2)` is Unfair. A newly arriving thread can
     *    steal a newly released permit before a thread that has been waiting in the queue
     *    for seconds. For FIFO ordering, you must use `new Semaphore(2, true)`.
     * 3. Uninterruptible Blocking: `acquire()` throws `InterruptedException`. If you need
     *    to acquire a permit without responding to thread interrupts, use `acquireUninterruptibly()`.
     */
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * 1. [Main] Initializes the `Semaphore` with 2 permits and starts Threads 1 through 5.
 * 2. [Threads 1 & 2] Arrive at `lock.acquire()`. Since 2 permits are available, BOTH
 *    threads instantly acquire a permit. Permit count drops to 0.
 * 3. [Threads 1 & 2] Enter the critical section, execute `isAvailable = true`, and
 *    enter a 4000ms sleep.
 * 4. [Threads 3, 4 & 5] Arrive at `lock.acquire()`. Because the permit count is 0,
 *    all three threads are parked by the OS and enter the WAITING state.
 * 5. [Threads 1 & 2] Wake up after 4000ms and execute the `finally` block. They call
 *    `lock.release()`. Permit count jumps back to 2.
 * 6. [Threads 3 & 4] The JVM wakes up two waiting threads. Threads 3 and 4 acquire
 *    the permits (count drops to 0), execute their work, and sleep for 4000ms.
 * 7. [Thread 5] Remains in the WAITING state because no permits are left.
 * 8. [Threads 3 & 4] Finish and release their permits.
 * 9. [Thread 5] Finally wakes up, acquires a permit, completes its execution, and releases.
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: Lock acquisition `acquire()` is O(1) in the uncontended path.
 *   If contention occurs, the thread is pushed to an internal wait queue managed
 *   by the AbstractQueuedSynchronizer (AQS).
 * - Space Complexity: O(1) heap allocation for the Semaphore object, which manages
 *   its wait queue internally.
 * - Performance Overhead: Semaphores are highly efficient because they rely on atomic
 *   Compare-And-Swap (Topics.CAS) operations to update the internal permit count integer.
 *   The primary overhead is OS context switching when threads are forced to block.
 */