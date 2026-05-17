package Topics.ThreadJoiningDeamonPriority.Join;
/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Coordinate thread execution order where a parent (main) thread must wait
 * for a child (producer) thread to complete its execution before proceeding.
 *
 * Real-World Use Case:
 * Application startup sequences. For example, a web server's main thread
 * spawning a background thread to load a large configuration file into memory
 * (or warm up a cache) and waiting for it to finish before accepting HTTP traffic.
 *
 * Concurrency Constraints:
 * - Execution Ordering: Main thread must not proceed until initialization is done.
 * - Memory Visibility: Changes made by the worker thread MUST be visible to the
 *   main thread without explicitly locking the read operation.
 */

public class ThreadJoinDemonstration {

    /**
     * Shared Resource representing our configuration or state.
     */
    static class SharedResource {
        // Notice: We DO NOT need 'volatile' or 'synchronized' here if this is
        // ONLY read by the main thread AFTER a successful Thread.join().
        // Thread.join() establishes a strict Happens-Before relationship.
        boolean isAvailable = false;

        public void produce() {
            System.out.println("[" + Thread.currentThread().getName() + "] Producer acquiring resource...");

            try {
                // Simulating heavy initialization work (e.g., DB connection, file I/O)
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                // Always restore the interrupt status in catch blocks!
                Thread.currentThread().interrupt();
                System.err.println("Producer was interrupted!");
            }

            this.isAvailable = true;
            System.out.println("[" + Thread.currentThread().getName() + "] Initialization complete. Resource available.");
        }
    }

    /**
     * ============================================================================
     * 2. IMPLEMENTATION & DEMONSTRATION
     * ============================================================================
     */
    public static void main(String[] args) {
        SharedResource sharedResource = new SharedResource();
        System.out.println("[" + Thread.currentThread().getName() + "] Main thread started. Spawning worker...");

        Thread producerThread = new Thread(() -> {
            sharedResource.produce();
        }, "Producer-Thread-1");

        // BUG FIX FROM ORIGINAL CODE: You must start the thread!
        // If start() is not called, isAlive() is false, and join() returns immediately.
        producerThread.start();

        try {
            System.out.println("[" + Thread.currentThread().getName() + "] Main thread calling join() and parking...");

            // The main thread is suspended here until producerThread terminates.
            producerThread.join();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }

        // ============================================================================
        // 3. IN-CODE DEEP DIVE
        // ============================================================================
        /*
         * WHY THIS APPROACH?
         * You requested low-level synchronization (Thread.join). However, in modern
         * Java (17+), creating raw OS threads is expensive. For production code, we
         * prefer `CountDownLatch(1)` or `CompletableFuture.runAsync(...).join()`.
         *
         * HAPPENS-BEFORE RELATIONSHIP (CRITICAL INTERVIEW CONCEPT):
         * Why is it safe to read `sharedResource.isAvailable` below without a lock?
         * According to the Java Memory Model (JMM), the termination of a thread
         * "Happens-Before" any successful return from a `join()` on that thread.
         * Therefore, all memory writes (isAvailable = true) made by 'Producer-Thread-1'
         * are flushed to main memory and guaranteed to be visible to the 'main' thread.
         *
         * EDGE CASES / PITFALLS:
         * 1. Missing .start(): Calling join() on an unstarted thread returns instantly.
         * 2. Infinite Waiting: Using the no-args join() means if the producer thread
         *    deadlocks or infinite loops, the main thread hangs forever. Production
         *    systems should use `join(long millis)` with a timeout.
         */

        System.out.println("[" + Thread.currentThread().getName() + "] Main thread resumed.");

        // This read is completely thread-safe due to the JMM join() guarantee.
        System.out.println("[" + Thread.currentThread().getName() + "] Is resource available? " + sharedResource.isAvailable);
        System.out.println("[" + Thread.currentThread().getName() + "] Main thread finishing its work.");
    }
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * 1. [Main Thread] Creates the `SharedResource` and `producerThread`.
 * 2. [Main Thread] Calls `producerThread.start()`. The JVM asks the OS to allocate a native thread.
 * 3. [Main Thread] Calls `producerThread.join()`. The JVM checks if producerThread `isAlive()`.
 *    Since it is, the main thread gives up its CPU time slice and enters the WAITING state.
 * 4. [Producer-Thread-1] Begins executing `produce()`. It sleeps for 2 seconds.
 * 5. [Producer-Thread-1] Wakes up, mutates `isAvailable = true`, and finishes execution.
 * 6. [JVM / OS] The thread terminates. The JVM notifies threads waiting on this thread object.
 * 7. [Main Thread] Wakes up, transitions back to RUNNABLE, regains CPU, and returns from `join()`.
 * 8. [Main Thread] Safely reads `isAvailable` (which is guaranteed to be true) and exits.
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: O(1) algorithmic complexity. Total time = Max(Main Thread time, Producer Thread time).
 * - Space Complexity: O(1) heap space. However, raw Threads consume ~1MB of native OS RAM
 *   for the thread stack.
 * - Performance Overhead: `Thread.join()` utilizes an underlying `Object.wait(0)` mechanic.
 *   The primary overhead here is the OS context switch to park the main thread and wake
 *   it back up. It is completely non-CPU intensive (no busy spinning), making it highly efficient
 *   for waiting on long-running tasks.
 */