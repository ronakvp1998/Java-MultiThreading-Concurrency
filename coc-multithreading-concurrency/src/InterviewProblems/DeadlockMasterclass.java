package InterviewProblems;

/**
 * ============================================================================
 * 1. HEADER & PROBLEM CONTEXT
 * ============================================================================
 * Problem Statement: Deadlock Simulation and Resolution
 *
 * Write a multithreaded Java program that guarantees a Deadlock between two
 * or more threads, and then provide a fixed version that resolves the issue.
 *
 * Context:
 * A Deadlock occurs when two or more threads are blocked forever, waiting for
 * each other. For a deadlock to occur, the four Coffman conditions must hold true
 * simultaneously:
 * 1. Mutual Exclusion: Resources cannot be shared.
 * 2. Hold and Wait: A thread holds a resource while waiting for another.
 * 3. No Preemption: Resources cannot be forcefully taken away.
 * 4. Circular Wait: Thread A waits for Thread B, which waits for Thread A.
 *
 * Constraints:
 * - Use basic intrinsic locks (`synchronized` blocks) to demonstrate.
 * - Ensure the deadlock reliably triggers in the simulation.
 *
 * Input/Output Formats:
 * - Output will show threads acquiring their first locks, and then halting
 *   indefinitely in the deadlock scenario, whereas the fixed scenario will
 *   complete execution smoothly.
 * ============================================================================
 */

public class DeadlockMasterclass {

    // Shared Resources
    private final Object resourceA = new Object();
    private final Object resourceB = new Object();

    /**
     * ============================================================================
     * 2.2 PROGRESSIVE IMPLEMENTATION ROADMAP (Non-DP)
     * ============================================================================
     * Phase 1: Optimal Approach - Fixed Deadlock (Lock Ordering)
     *
     * Intuition:
     * The most robust way to prevent deadlocks is to eliminate the "Circular Wait"
     * condition. We do this by enforcing a strict **Global Lock Ordering**.
     * If ALL threads across the entire application always acquire `resourceA`
     * before `resourceB`, a cycle can never form. Thread 2 will simply block
     * waiting for `resourceA` before it even has a chance to grab `resourceB`.
     *
     * Complexity Analysis:
     * - Time Complexity: O(1) for lock acquisition.
     * - Space Complexity: O(1) auxiliary space.
     * ============================================================================
     */
    public void executeWithoutDeadlock() throws InterruptedException {
        System.out.println("Starting Phase 1: Deadlock-Free Execution (Lock Ordering)");

        Thread thread1 = new Thread(() -> {
            synchronized (resourceA) {
                System.out.println("Fixed-Thread-1: Locked Resource A");
                sleepSilently(50); // Simulate work, allowing thread 2 to start

                synchronized (resourceB) {
                    System.out.println("Fixed-Thread-1: Locked Resource B");
                }
            }
        });

        Thread thread2 = new Thread(() -> {
            // FIX: Enforce the same lock acquisition order as Thread 1
            synchronized (resourceA) {
                System.out.println("Fixed-Thread-2: Locked Resource A");
                sleepSilently(50);

                synchronized (resourceB) {
                    System.out.println("Fixed-Thread-2: Locked Resource B");
                }
            }
        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();
        System.out.println("Phase 1 Complete: No Deadlock occurred!\n");
    }

    /**
     * ============================================================================
     * Phase 2: Brute Force Approach - Guaranteed Deadlock
     *
     * Intuition:
     * To guarantee a deadlock, we intentionally create a Circular Wait.
     * - Thread 1 locks Resource A, then sleeps (holding A).
     * - Thread 2 locks Resource B, then sleeps (holding B).
     * - Thread 1 wakes up and tries to lock Resource B (blocked by Thread 2).
     * - Thread 2 wakes up and tries to lock Resource A (blocked by Thread 1).
     * Both threads are now blocked forever.
     *
     * Complexity Analysis:
     * - Time Complexity: O(Infinity) - The application halts forever.
     * - Space Complexity: O(1)
     * ============================================================================
     */
    public void executeWithDeadlock() {
        System.out.println("Starting Phase 2: Guaranteed Deadlock Generation");
        System.out.println("WARNING: The JVM will hang after printing the lock acquisitions.");

        Thread thread1 = new Thread(() -> {
            synchronized (resourceA) {
                System.out.println("Deadlock-Thread-1: Locked Resource A");

                // Sleep guarantees Thread 2 has time to lock Resource B
                sleepSilently(50);

                System.out.println("Deadlock-Thread-1: Waiting for Resource B...");
                synchronized (resourceB) {
                    System.out.println("Deadlock-Thread-1: Locked Resource B");
                }
            }
        });

        Thread thread2 = new Thread(() -> {
            // THE MISTAKE: Acquiring locks in the reverse order
            synchronized (resourceB) {
                System.out.println("Deadlock-Thread-2: Locked Resource B");

                // Sleep guarantees Thread 1 has time to lock Resource A
                sleepSilently(50);

                System.out.println("Deadlock-Thread-2: Waiting for Resource A...");
                synchronized (resourceA) {
                    System.out.println("Deadlock-Thread-2: Locked Resource A");
                }
            }
        });

        thread1.start();
        thread2.start();
    }

    /**
     * ============================================================================
     * Phase 3: Alternative Approaches to Fix Deadlocks
     *
     * 1. Lock Timeouts: Instead of intrinsic `synchronized` blocks, use
     *    `ReentrantLock.tryLock(long timeout, TimeUnit unit)`. If a thread cannot
     *    acquire the second lock within the timeout, it releases its first lock
     *    (backing off) and retries later, breaking the "Hold and Wait" condition.
     *
     * 2. Thread Interruption: While threads waiting on `synchronized` blocks cannot
     *    be interrupted, threads waiting on `ReentrantLock.lockInterruptibly()` can
     *    be terminated to break the cycle.
     * ============================================================================
     */

    // Helper method to keep code clean
    private void sleepSilently(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * ============================================================================
     * 4. TESTING SUITE
     * ============================================================================
     */
    public static void main(String[] args) throws InterruptedException {
        DeadlockMasterclass simulator = new DeadlockMasterclass();

        // Run the fixed version first so we can see it complete successfully
        simulator.executeWithoutDeadlock();

        // Run the deadlocked version (This will freeze the application)
        simulator.executeWithDeadlock();

        // Note: Code below this point will never be reached because the main thread
        // does not join the deadlocked threads, but the JVM will stay alive because
        // the non-daemon deadlocked threads never terminate.
    }
}