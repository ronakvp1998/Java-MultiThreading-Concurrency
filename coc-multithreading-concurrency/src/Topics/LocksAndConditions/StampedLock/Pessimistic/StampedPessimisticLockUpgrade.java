package Topics.LocksAndConditions.StampedLock.Pessimistic;

/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Coordinate highly concurrent access to a shared resource where threads primarily
 * read data, but occasionally need to mutate it based on the read condition (a
 * "Check-then-Act" scenario). Using a `ReentrantReadWriteLock` strictly forbids
 * upgrading a Read Lock to a Write Lock—attempting to do so causes an instant
 * and permanent deadlock.
 *
 * Real-World Use Case:
 * Distributed Session Management or In-Memory Authentication Caches. Millions of
 * requests validate a JWT or session token (heavy reads). If the token is expired,
 * the thread must instantly fetch a new one and update the cache (write). If 10
 * threads detect the expiration simultaneously, only ONE should perform the update
 * while the others wait and then read the new value.
 *
 * Concurrency Constraints:
 * - Lock Upgrading: Must safely transition from a read state to a write state
 *   without dropping the lock completely (which would allow race conditions).
 * - Non-Reentrancy: `StampedLock` is strictly non-reentrant. If a thread holds
 *   a lock and calls a method that attempts to acquire the same lock, it deadlocks.
 */

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

public class StampedPessimisticLockUpgrade {

    /**
     * Shared Resource representing an expiring Authentication Token Cache.
     */
    static class SessionCache {
        private String authToken = "INIT_TOKEN_XYZ";
        private long expirationTimeMs = System.currentTimeMillis() + 500; // Expires in 500ms

        private final StampedLock lock = new StampedLock();

        /**
         * SENIOR PATTERN: Conditional Lock Upgrading.
         * This demonstrates the true power of StampedLock's pessimistic mode.
         */
        public String getValidToken() {
            // 1. Acquire a strict pessimistic read lock. Blocks if a write is active.
            long stamp = lock.readLock();

            try {
                // 2. The Check-then-Act loop
                while (System.currentTimeMillis() > expirationTimeMs) {
                    System.out.println("[" + Thread.currentThread().getName() + "] Token expired. Attempting to upgrade lock...");

                    // 3. ATTEMPT UPGRADE: Try to seamlessly convert Read -> Write
                    long writeStamp = lock.tryConvertToWriteLock(stamp);

                    if (writeStamp != 0L) {
                        // SUCCESS: No other threads were holding a read lock.
                        // We now hold an exclusive WRITE lock.
                        stamp = writeStamp;

                        System.out.println(">>> [" + Thread.currentThread().getName() + "] Lock UPGRADED successfully. Generating new token.");

                        // Mutate state
                        this.authToken = "NEW_TOKEN_" + System.currentTimeMillis();
                        this.expirationTimeMs = System.currentTimeMillis() + 2000;

                        // Simulate network call to auth server
                        simulateNetworkDelay();

                        break; // Work complete, exit the while loop

                    } else {
                        // FAILURE: Another thread also holds a read lock right now.
                        // We cannot upgrade without deadlocking. We MUST drop our read
                        // lock and manually queue for a standard write lock.
                        System.out.println("--- [" + Thread.currentThread().getName() + "] Upgrade failed (contention). Queuing for Write Lock.");

                        lock.unlockRead(stamp);
                        stamp = lock.writeLock(); // This blocks until all readers finish

                        // NOTE: The loop automatically restarts here! We must re-check the
                        // expiration condition because another thread might have updated
                        // the token while we were blocked waiting for the writeLock.
                    }
                }

                // Return the valid token (whether it was just refreshed or was already valid)
                return this.authToken;

            } finally {
                // 4. CRITICAL VERSATILITY: `unlock(stamp)` automatically knows if the
                // provided stamp represents a read lock or a write lock and releases
                // the correct one.
                lock.unlock(stamp);
            }
        }

        private void simulateNetworkDelay() {
            try { Thread.sleep(200); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    /**
     * ============================================================================
     * 2. IMPLEMENTATION & DEMONSTRATION
     * ============================================================================
     */
    public static void main(String[] args) throws InterruptedException {
        SessionCache cache = new SessionCache();
        ExecutorService executor = Executors.newFixedThreadPool(5);

        System.out.println("Phase 1: Valid Token Reads (Uncontended)\n");
        for (int i = 0; i < 3; i++) {
            executor.submit(() -> {
                System.out.println("[" + Thread.currentThread().getName() + "] Read: " + cache.getValidToken());
            });
        }

        // Force the token to expire
        Thread.sleep(600);
        System.out.println("\nPhase 2: Token Expired! Massive Contention Scenario\n");

        // Release 5 threads simultaneously that will all detect the expiration
        for (int i = 0; i < 5; i++) {
            executor.submit(() -> {
                String token = cache.getValidToken();
                System.out.println("[" + Thread.currentThread().getName() + "] Safely acquired valid token: " + token);
            });
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // ============================================================================
        // 3. IN-CODE DEEP DIVE
        // ============================================================================
        /*
         * WHY STAMPED LOCK PESSIMISTIC UPGRADE?
         * If you attempt this with `ReentrantReadWriteLock`, a thread holding a ReadLock
         * that calls `writeLock.lock()` will deadlock forever. You are forced to call
         * `readLock.unlock()` -> `writeLock.lock()`. In the gap between unlocking and
         * locking, another thread can alter the state, leading to redundant network calls
         * or corrupted data. `tryConvertToWriteLock` eliminates that gap atomically.
         *
         * HAPPENS-BEFORE RELATIONSHIPS:
         * Releasing any write lock via `unlock(stamp)` Happens-Before any subsequent
         * successful `readLock()` or `writeLock()`. The JMM flushes the newly minted
         * `authToken` and `expirationTimeMs` to main memory, guaranteeing visibility.
         *
         * PITFALLS TO DISCUSS IN INTERVIEWS:
         * 1. The Lost Stamp: Overwriting the `stamp` variable improperly inside the `while`
         *    loop will result in an `IllegalMonitorStateException` in the `finally` block.
         * 2. Missing Re-Evaluation: When `tryConvertToWriteLock` fails, you acquire a
         *    `writeLock()`. You MUST loop back and re-evaluate your business condition
         *    (`currentTime > expirationTime`). Failing to do so causes the "Double-Checked
         *    Locking" bug, where multiple threads redundantly refresh the token.
         */
    }
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * [Phase 2: Contention Execution Flow]
 * 1. [Threads 1-5] All call `readLock()` simultaneously. Since it's a read lock,
 *    all 5 threads enter the critical section concurrently.
 * 2. [Threads 1-5] All detect that `currentTime > expirationTimeMs`.
 * 3. [Threads 1-5] All call `tryConvertToWriteLock(stamp)`.
 * 4. [Hardware Level] Because multiple read locks are actively held, the upgrade
 *    FAILS for all threads. `writeStamp` returns 0.
 * 5. [Threads 1-5] All enter the `else` block. They drop their read locks and queue
 *    up for an exclusive `writeLock()`.
 * 6. [Thread 1] Wins the race. Acquires the strict write lock.
 * 7. [Thread 1] Loops back to `while(currentTime > expiration)`. It is still true.
 * 8. [Thread 1] Calls `tryConvertToWriteLock()`. Since it ALREADY holds a write lock,
 *    this trivially succeeds and returns the exact same stamp.
 * 9. [Thread 1] Generates the new token, updates expiration, breaks loop, and UNLOCKS.
 * 10. [Thread 2] Senses the unlock. Wakes up and acquires the `writeLock()`.
 * 11. [Thread 2] Loops back to `while(currentTime > expiration)`. Because Thread 1
 *     just updated the expiration, this condition is now FALSE.
 * 12. [Thread 2] Bypasses the generation block completely, returns the token generated
 *     by Thread 1, and UNLOCKS. (Threads 3-5 repeat this exact bypass).
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: O(1) for acquisition. The `tryConvertToWriteLock` operates via
 *   a fast atomic Compare-And-Swap (Topics.CAS) operation.
 * - Space Complexity: O(1) primitive allocation. It creates less garbage collection
 *   pressure than ReentrantLocks because it doesn't allocate AQS Node objects for
 *   uncontended locks.
 * - Overhead: Higher cognitive overhead for the developer, but the lowest CPU
 *   overhead for lock-upgrading semantics in the JDK.
 */