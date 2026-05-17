package Topics.ThreadJoiningDeamonPriority.Join;

/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Guarantee strict sequential execution of threads: Thread-1 executes first,
 * followed by Thread-2, and finally the Main thread resumes.
 *
 * Real-World Use Case:
 * Data pipeline stages. For example: Thread-1 downloads a file from S3,
 * Thread-2 decrypts and parses that file, and the Main thread aggregates
 * the results to return an HTTP response.
 *
 * Concurrency Constraints:
 * - Execution Ordering: T1 MUST finish before T2 starts its core logic.
 * - Non-Blocking Start: We must be able to call .start() on all threads
 *   simultaneously without breaking the required sequence.
 */

public class ThreadSequencing {

    /**
     * Shared Resource simulating our pipeline data.
     */
    static class PipelineContext {
        String data = "Empty";

        public void downloadData() {
            System.out.println("[" + Thread.currentThread().getName() + "] Downloading data...");
            simulateWork(1000);
            this.data = "Downloaded_Raw_Data";
            System.out.println("[" + Thread.currentThread().getName() + "] Download complete.");
        }

        public void decryptData() {
            System.out.println("[" + Thread.currentThread().getName() + "] Decrypting: " + this.data);
            simulateWork(1000);
            this.data = "Decrypted_Clear_Data";
            System.out.println("[" + Thread.currentThread().getName() + "] Decryption complete.");
        }

        private void simulateWork(int millis) {
            try { Thread.sleep(millis); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    /**
     * ============================================================================
     * 2. IMPLEMENTATION & DEMONSTRATION
     * ============================================================================
     */
    public static void main(String[] args) {
        PipelineContext context = new PipelineContext();
        System.out.println("[" + Thread.currentThread().getName() + "] Pipeline started.");

        // Stage 1: Download Thread
        Thread thread1 = new Thread(() -> {
            context.downloadData();
        }, "Thread-1");

        // Stage 2: Decrypt Thread
        // Notice we pass thread1's reference into thread2's lambda
        Thread thread2 = new Thread(() -> {
            try {
                System.out.println("[" + Thread.currentThread().getName() + "] Waiting for Thread-1 to finish...");

                // Thread-2 immediately blocks itself until Thread-1 dies
                thread1.join();

                // Thread-1 is dead. Thread-2 executes its logic.
                context.decryptData();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Thread-2 was interrupted while waiting for Thread-1");
            }
        }, "Thread-2");

        // We can start them in ANY order, the sequence is still guaranteed.
        // Even if Thread-2 gets CPU time first, it will immediately hit thread1.join() and park.
        thread2.start();
        thread1.start();

        try {
            System.out.println("[" + Thread.currentThread().getName() + "] Waiting for Thread-2 to finish...");

            // The main thread ONLY waits for thread2.
            // By transit property, if T2 is done, T1 is also done.
            thread2.join();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("[" + Thread.currentThread().getName() + "] Pipeline complete. Final Data: " + context.data);

        // ============================================================================
        // 3. IN-CODE DEEP DIVE
        // ============================================================================
        /*
         * WHY THIS APPROACH?
         * Using "Chained Joins" is the standard low-level OS threading solution to strict
         * sequencing. It guarantees order without needing complex `wait()/notify()` blocks
         * or `synchronized` locks.
         *
         * HAPPENS-BEFORE RELATIONSHIPS (The Transitive Property):
         * 1. T1 terminates Happens-Before T2 returns from `thread1.join()`.
         *    (T2 safely sees T1's writes).
         * 2. T2 terminates Happens-Before Main returns from `thread2.join()`.
         *    (Main safely sees T2's writes).
         * 3. Because Happens-Before is transitive: T1 writes -> T2 writes -> Main reads.
         *    No `volatile` or `synchronized` is required for `context.data`!
         *
         * THE MODERN "SENIOR" ALTERNATIVE (CompletableFuture):
         * In Java 8+, raw threads are discouraged for sequencing. An interviewer will expect
         * this implementation instead:
         *
         * CompletableFuture<Void> pipeline = CompletableFuture
         *      .runAsync(() -> context.downloadData())      // Thread-1 equivalent
         *      .thenRunAsync(() -> context.decryptData());  // Thread-2 equivalent (only runs after T1)
         *
         * pipeline.join(); // Main thread equivalent
         */
    }
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * 1. [Main Thread] Creates T1 and T2.
 * 2. [Main Thread] Calls T2.start() and T1.start().
 * 3. [Main Thread] Calls T2.join() -> Enters WAITING state.
 * 4. [Thread-2] Wakes up, prints its waiting message, calls T1.join() -> Enters WAITING state.
 * 5. [Thread-1] Wakes up, executes `downloadData()`, mutates `context.data`, and terminates.
 * 6. [Thread-2] Senses T1 terminated. Returns from T1.join() -> Enters RUNNABLE.
 * 7. [Thread-2] Executes `decryptData()`, mutates `context.data`, and terminates.
 * 8. [Main Thread] Senses T2 terminated. Returns from T2.join() -> Enters RUNNABLE.
 * 9. [Main Thread] Safely prints the fully decrypted `context.data` and exits.
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: O(1) algorithmic complexity. Total Time = T1 execution + T2 execution.
 * - Space Complexity: O(1) heap, but allocates native memory for 2 thread stacks (~2MB).
 * - Overhead: Negligible CPU overhead because `join()` parks the threads efficiently at the
 *   OS level rather than busy-spinning. However, using raw Threads for small tasks is
 *   expensive compared to thread-pool architectures (like `CompletableFuture`).
 */