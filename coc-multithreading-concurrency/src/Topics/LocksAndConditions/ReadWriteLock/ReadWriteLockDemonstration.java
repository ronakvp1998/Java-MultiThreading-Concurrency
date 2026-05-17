package Topics.LocksAndConditions.ReadWriteLock;

/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Optimize concurrent access to a shared resource that experiences a very high
 * volume of read operations but infrequent write operations. Using a standard
 * `synchronized` block or `ReentrantLock` forces read operations to execute
 * sequentially, creating a massive artificial bottleneck.
 *
 * Real-World Use Case:
 * - In-Memory Caches (e.g., local application cache, DNS resolution cache).
 * - Configuration Managers where settings are read millions of times a minute
 *   but updated by an admin only once a day.
 * - Read-heavy database connection pool state managers.
 *
 * Concurrency Constraints:
 * - Parallel Reads: Multiple threads must be able to read simultaneously.
 * - Exclusive Writes: A writing thread must block ALL other threads (both
 *   readers and writers) to prevent data corruption or stale reads.
 * - Starvation Avoidance: A continuous stream of readers must not prevent a
 *   writer from ever acquiring the lock (Writer Starvation).
 */

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.Lock;

public class ReadWriteLockDemonstration {

    /**
     * Shared Resource representing an In-Memory Configuration Cache.
     */
    static class ConcurrentCache {

        // The underlying data structure is NOT thread-safe by itself.
        private final Map<String, String> cache = new HashMap<>();

        // SENIOR UPGRADE: We pass 'true' to enable Fairness.
        // This ensures the lock is granted to threads in FIFO order, completely
        // eliminating the risk of Writer Starvation at the cost of some throughput.
        private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);

        private final Lock readLock = rwLock.readLock();
        private final Lock writeLock = rwLock.writeLock();

        /**
         * Simulates a Consumer reading from the cache.
         */
        public String readData(String key) {
            readLock.lock();
            try {
                System.out.println("[" + Thread.currentThread().getName() + "] READ Lock ACQUIRED. Reading key: " + key);
                // Simulate I/O or processing time to prove parallel reads
                Thread.sleep(1000);
                return cache.get(key);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            } finally {
                readLock.unlock();
                System.out.println("[" + Thread.currentThread().getName() + "] READ Lock RELEASED.");
            }
        }

        /**
         * Simulates a Producer writing/updating the cache.
         */
        public void writeData(String key, String value) {
            writeLock.lock();
            try {
                System.out.println(">>> [" + Thread.currentThread().getName() + "] WRITE Lock ACQUIRED. Updating key: " + key);
                // Simulate heavy write operation
                Thread.sleep(2000);
                cache.put(key, value);
                System.out.println(">>> [" + Thread.currentThread().getName() + "] WRITE operation complete.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                writeLock.unlock();
                System.out.println(">>> [" + Thread.currentThread().getName() + "] WRITE Lock RELEASED.");
            }
        }
    }

    /**
     * ============================================================================
     * 2. IMPLEMENTATION & DEMONSTRATION
     * ============================================================================
     */
    public static void main(String[] args) throws InterruptedException {
        ConcurrentCache configCache = new ConcurrentCache();

        // Pre-populate the cache
        configCache.writeData("FeatureToggle", "ENABLED");

        System.out.println("\n--- Starting Concurrent Access Test ---\n");

        // Spawn 3 Reader Threads
        Runnable readTask = () -> configCache.readData("FeatureToggle");
        Thread r1 = new Thread(readTask, "Reader-1");
        Thread r2 = new Thread(readTask, "Reader-2");
        Thread r3 = new Thread(readTask, "Reader-3");

        // Spawn 1 Writer Thread
        Thread w1 = new Thread(() -> configCache.writeData("FeatureToggle", "DISABLED"), "Writer-1");

        // Start readers. You will see they acquire the lock simultaneously!
        r1.start();
        r2.start();

        Thread.sleep(100); // Give readers a tiny head start

        // Start writer. It MUST wait for r1 and r2 to finish.
        w1.start();

        Thread.sleep(100);

        // Start another reader. Because the lock is FAIR, r3 will queue up
        // BEHIND the writer. It will not bypass the writer just because it's a reader.
        r3.start();

        // ============================================================================
        // 3. IN-CODE DEEP DIVE
        // ============================================================================
        /*
         * WHY REENTRANT-READ-WRITE-LOCK OVER SYNCHRONIZED?
         * Using `synchronized` on `readData` would force Reader-1 and Reader-2 to
         * wait for each other. In a system with 10,000 reads per second, this destroys
         * latency. `ReadWriteLock` allows the Read Lock to be held by multiple threads
         * simultaneously, provided no thread holds the Write Lock.
         *
         * HAPPENS-BEFORE RELATIONSHIP:
         * Releasing the Write Lock Happens-Before the acquisition of a subsequent
         * Read Lock. When `Writer-1` unlocks, the JMM flushes the updated `cache`
         * state to main memory. When `Reader-3` acquires the read lock, it is
         * guaranteed to see the value "DISABLED" and not a stale CPU cache value.
         *
         * PITFALLS TO DISCUSS IN INTERVIEWS:
         * 1. Lock Upgrading (DEADLOCK): A thread holding a Read Lock CANNOT acquire
         *    a Write Lock without releasing the Read Lock first. Attempting to do so
         *    will cause a permanent deadlock.
         * 2. Lock Downgrading (SAFE): A thread holding a Write Lock CAN acquire a
         *    Read Lock, and then release its Write Lock. This is useful for keeping
         *    read access immediately after mutating state.
         * 3. Writer Starvation: In default (unfair) mode, if a system is bombarded
         *    by readers, the read lock is never fully released, meaning a writer might
         *    wait forever. We fix this by passing `true` (fairness) to the constructor.
         */
    }
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * 1. [Main] Populates initial cache value.
 * 2. [Reader-1 & Reader-2] Both call `readData()`. Because no write lock is held,
 *    BOTH threads successfully acquire the read lock at the exact same time and
 *    sleep for 1000ms.
 * 3. [Writer-1] Calls `writeData()`. It attempts to acquire the write lock.
 *    Because read locks are actively held, it enters the WAITING state.
 * 4. [Reader-3] Calls `readData()`. Because we used a Fair Lock, and Writer-1
 *    is already queued, Reader-3 does NOT get the lock immediately. It queues
 *    up behind Writer-1.
 * 5. [Reader-1 & Reader-2] Finish reading and call `readLock.unlock()`.
 * 6. [Writer-1] Wakes up, acquires exclusive write access, mutates the cache,
 *    and releases the write lock. Memory is flushed.
 * 7. [Reader-3] Wakes up, acquires the read lock, and safely reads the newly
 *    updated value.
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: Lock acquisition is O(1).
 * - Space Complexity: O(1) heap allocation for the AQS node queue.
 * - Performance Overhead: `ReentrantReadWriteLock` is significantly more complex
 *   than `ReentrantLock`. The JVM has to track the count of readers. If your read
 *   operations are extremely fast (e.g., nanoseconds), the overhead of updating the
 *   AQS state might actually be slower than just using a standard `ReentrantLock`.
 *   RW-Locks are only beneficial when the critical section (the reading part) takes
 *   a relatively long time.
 * - Senior Note: In Java 8+, `StampedLock` is often preferred over `ReentrantReadWriteLock`
 *   for even higher read throughput via "Optimistic Reading", though it is more complex to implement.
 */