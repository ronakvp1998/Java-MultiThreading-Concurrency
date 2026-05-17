package concurrency.executors;

import java.util.concurrent.*;

/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Demonstrate the complete lifecycle and API of the java.util.concurrent.Future
 * interface using a ThreadPoolExecutor. The code must handle task submission,
 * asynchronous result retrieval, timeouts, task cancellation, and exception handling.
 *
 * Real-World Use Case:
 * "Scatter-Gather" patterns in microservices. For example, a travel booking engine
 * needing to fetch flight prices, hotel rates, and car rentals simultaneously.
 * The main thread delegates these tasks to worker threads, receives Future handles,
 * and later gathers the results, enforcing strict timeouts to meet API SLAs.
 *
 * Concurrency Constraints:
 * 1. Blocking Operations: Future.get() blocks the calling thread until completion.
 *    Careless use can lead to thread starvation or system deadlocks.
 * 2. Visibility: Changes made by the worker thread must be visible to the thread
 *    calling get().
 * 3. Interruption: Canceling a running task requires the worker thread to safely
 *    handle InterruptedException to avoid corrupted state.
 * ============================================================================
 */
public class FutureLifecycleDemo {

    public static void main(String[] args) {
        /*
         * ============================================================================
         * 3. IN-CODE DEEP DIVE: CONFIGURATION & HAPPENS-BEFORE
         * ============================================================================
         * Deep Dive: Why Callable over Runnable?
         * The user's original code submitted a Runnable, which inherently returns a
         * Future<?> that yields 'null' upon completion. To return a meaningful
         * result across thread boundaries without shared mutable state, we use Callable<V>.
         *
         * Happens-Before Relationship:
         * JLS Sec 17.4.5:
         * 1. The actions in a thread prior to calling executor.submit() happen-before
         *    the execution of the task begins in the worker thread.
         * 2. The completion of the asynchronous task happens-before the successful
         *    return from the corresponding Future.get(). This guarantees the main thread
         *    sees the fully constructed result without volatile/synchronized keywords.
         * ============================================================================
         */
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(
                2,                                      // corePoolSize
                4,                                      // maximumPoolSize
                1,                                      // keepAliveTime
                TimeUnit.HOURS,                         // unit
                new ArrayBlockingQueue<>(10),           // workQueue (bounded)
                Executors.defaultThreadFactory(),       // threadFactory
                new ThreadPoolExecutor.AbortPolicy()    // handler for rejection
        );

        System.out.println("--- Starting Future Lifecycle Demonstrations ---\n");

        /*
         * SCENARIO 1: Standard Completion (get and isDone)
         */
        System.out.println("[Scenario 1] Standard Callable Execution:");
        Future<String> standardFuture = poolExecutor.submit(() -> {
            Thread.sleep(500); // Simulate network I/O
            return "Task 1 Data Processed successfully.";
        });

        System.out.println("Is Task 1 done instantly? " + standardFuture.isDone());
        try {
            // BLOCKING CALL: Main thread pauses here until worker finishes.
            String result1 = standardFuture.get();
            System.out.println("Task 1 Result: " + result1);
            System.out.println("Is Task 1 done now? " + standardFuture.isDone() + "\n");
        } catch (InterruptedException | ExecutionException e) {
            handleException(e);
        }

        /*
         * SCENARIO 2: Timeout Handling (get with parameters)
         * Pitfall Avoided: Never use parameterless get() in production systems
         * without a timeout guarantee, as a hung worker thread will hang the main thread permanently.
         */
        System.out.println("[Scenario 2] Timeout Enforcement:");
        Future<String> timeoutFuture = poolExecutor.submit(() -> {
            Thread.sleep(3000); // Simulating a slow database query (3 seconds)
            return "Task 2 Data";
        });

        try {
            // We only give it 1 second to finish. It will throw TimeoutException.
            String result2 = timeoutFuture.get(1, TimeUnit.SECONDS);
            System.out.println("Task 2 Result: " + result2);
        } catch (TimeoutException e) {
            System.err.println("Task 2 timed out! SLA breached. Cancelling...");
            timeoutFuture.cancel(true); // Attempt to interrupt the slow thread
        } catch (InterruptedException | ExecutionException e) {
            handleException(e);
        }
        System.out.println();

        /*
         * SCENARIO 3: Task Cancellation (cancel, isCancelled)
         */
        System.out.println("[Scenario 3] Task Cancellation:");
        Future<String> cancelFuture = poolExecutor.submit(() -> {
            try {
                Thread.sleep(5000); // Long running task
            } catch (InterruptedException e) {
                // Good Practice: Always restore the interrupted status
                Thread.currentThread().interrupt();
                return "Task 3 Interrupted during execution";
            }
            return "Task 3 Completed";
        });

        // Cancel the task. 'true' means interrupt the thread if it has already started.
        // 'false' means cancel only if it's still in the queue (hasn't started executing).
        boolean cancelSuccess = cancelFuture.cancel(true);
        System.out.println("Cancel command issued successfully? " + cancelSuccess);
        System.out.println("Is Task 3 cancelled? " + cancelFuture.isCancelled());
        System.out.println("Is Task 3 done? (Cancelled implies done): " + cancelFuture.isDone() + "\n");

        /*
         * SCENARIO 4: Exception Propagation (ExecutionException)
         * Deep Dive: If a worker thread throws a RuntimeException, it doesn't crash
         * the JVM. The Executor catches it and wraps it in an ExecutionException,
         * throwing it only when get() is called.
         */
        System.out.println("[Scenario 4] Exception Propagation:");
        Future<Integer> errorFuture = poolExecutor.submit(() -> {
            return 10 / 0; // Deliberate ArithmeticException
        });

        try {
            errorFuture.get();
        } catch (ExecutionException e) {
            System.err.println("Task 4 threw an exception during execution:");
            System.err.println("Root Cause: " + e.getCause().toString() + "\n");
        } catch (InterruptedException e) {
            handleException(e);
        }

        /*
         * ============================================================================
         * 4. STEP-BY-STEP EXECUTION LOGIC (SHUTDOWN)
         * ============================================================================
         * 1. poolExecutor.shutdown() transitions pool to SHUTDOWN state.
         * 2. No new tasks are accepted.
         * 3. Previously submitted tasks (in queue or running) continue to execute.
         * 4. awaitTermination() blocks main thread until workers finish or timeout hits.
         * 5. If timeout hits, shutdownNow() is called to send interrupts to all workers.
         * ============================================================================
         */
        poolExecutor.shutdown();
        try {
            if (!poolExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                poolExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            poolExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        System.out.println("Main thread execution completed safely.");
    }

    private static void handleException(Exception e) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            System.err.println("Main thread was interrupted while waiting.");
        } else {
            System.err.println("Execution failed: " + e.getMessage());
        }
    }
}

/*
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity:
 *   Task submission: O(1) amortized.
 *   Result Retrieval: O(T) where T is the time remaining for the worker thread
 *   to complete the task. The get() method uses a `LockSupport.park()` mechanism
 *   under the hood (via WaitNodes), ensuring the waiting thread doesn't consume
 *   CPU cycles (no busy-waiting).
 *
 * - Space Complexity:
 *   O(1) auxiliary overhead per task. A FutureTask object is instantiated for
 *   each submitted Callable to hold the state, outcome, and caller thread references.
 *
 * - Synchronization Overhead:
 *   FutureTask relies heavily on the `state` variable (NEW, COMPLETING, NORMAL,
 *   EXCEPTIONAL, CANCELLED, INTERRUPTING, INTERRUPTED). State transitions are
 *   managed using highly optimized Topics.CAS (Compare-And-Swap) operations via VarHandles
 *   (or Unsafe in older JDKs), completely avoiding OS-level locking overhead for
 *   state changes.
 *
 * Modern Alternative:
 * For purely asynchronous, non-blocking pipelines, `CompletableFuture` (introduced
 * in Java 8) is vastly superior to raw `Future` because it allows chaining callbacks
 * (e.g., thenApply, thenAccept) without requiring a thread to block on `get()`.
 * ============================================================================
 */