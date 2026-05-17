package Topics.ThreadPollsExecutor;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Demonstrate the architectural differences, exception handling mechanisms, and
 * return-value pipelines of submitting both `Runnable` and `Callable` tasks to a
 * custom, production-grade `ThreadPoolExecutor`.
 *
 * Real-World Use Case:
 * A high-throughput microservice handling two types of workloads:
 * 1. Fire-and-Forget (Runnable): Asynchronously writing telemetry logs to disk
 *    or pushing metrics to DataDog. The main thread doesn't need a response.
 * 2. Compute-and-Return (Callable): Aggregating user profile data from a slow
 *    database and calculating a credit score. The main thread MUST block and
 *    wait for this critical data before returning an HTTP 200 response.
 *
 * Concurrency Constraints:
 * 1. Exception Masking: Unhandled exceptions in `Runnable` kill the worker thread
 *    if not caught internally. In `Callable`, they are safely wrapped in an
 *    ExecutionException and propagated to the calling thread.
 * 2. Visibility: Task submission happens-before execution. The completion of the
 *    task happens-before `Future.get()` returns.
 * ============================================================================
 */
public class RunnableVsCallableDemo {

    public static void main(String[] args) {
        /*
         * ============================================================================
         * 3. IN-CODE DEEP DIVE: CONFIGURATION & HAPPENS-BEFORE
         * ============================================================================
         * We construct a strictly bounded ThreadPoolExecutor to prevent OOM under load.
         *
         * Happens-Before Relationship (JLS Sec 17.4.5):
         * When `executor.submit()` is called, all memory writes made by the main
         * thread prior to the submission are guaranteed to be visible to the
         * worker thread executing the Runnable/Callable.
         * ============================================================================
         */
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2,                                      // corePoolSize
                4,                                      // maximumPoolSize
                30,                                     // keepAliveTime
                TimeUnit.SECONDS,                       // unit
                new ArrayBlockingQueue<>(10),           // workQueue
                new NamedThreadFactory(),               // custom thread factory
                new ThreadPoolExecutor.CallerRunsPolicy() // Handle overload by slowing down the producer
        );

        System.out.println("--- Starting Runnable vs Callable Execution ---\n");

        /*
         * ------------------------------------------------------------------------
         * APPROACH 1: RUNNABLE (Fire-and-Forget)
         * ------------------------------------------------------------------------
         * Deep Dive: Runnable returns void. It cannot return a value, and it cannot
         * throw checked exceptions. All exceptions must be caught inside the run() block.
         */
        System.out.println("[Submitting Runnable Task...]");
        Runnable logTask = () -> {
            String threadName = Thread.currentThread().getName();
            System.out.println(threadName + " [Runnable]: Writing telemetry logs to disk...");
            try {
                Thread.sleep(500); // Simulate I/O
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println(threadName + " [Runnable]: Interrupted!");
            }
            // Cannot return anything here.
        };

        // When submitting a Runnable, Future<?> is returned.
        // Calling get() will just return 'null' upon successful completion.
        Future<?> runnableFuture = executor.submit(logTask);

        /*
         * ------------------------------------------------------------------------
         * APPROACH 2: CALLABLE (Compute-and-Return)
         * ------------------------------------------------------------------------
         * Deep Dive: Callable<V> returns type V and throws Exception. The JVM uses
         * the Adapter Pattern (wrapping it in a FutureTask) so the worker thread
         * can safely execute it, capture the result, and pass it across thread
         * boundaries back to the main thread.
         */
        System.out.println("[Submitting Callable Task...]");
        Callable<String> dbTask = () -> {
            String threadName = Thread.currentThread().getName();
            System.out.println(threadName + " [Callable]: Fetching user profile from DB...");
            Thread.sleep(1000); // Simulate network latency

            // Callable CAN return a result
            return "User Profile: { id: 101, name: \"Alice\", status: \"ACTIVE\" }";
        };

        // When submitting a Callable, Future<T> is returned containing the exact type.
        Future<String> callableFuture = executor.submit(dbTask);

        /*
         * ============================================================================
         * 4. STEP-BY-STEP EXECUTION LOGIC (Gathering Results)
         * ============================================================================
         * 1. Main thread calls runnableFuture.get(). Blocks until Runnable finishes.
         * 2. Runnable completes, get() returns null.
         * 3. Main thread calls callableFuture.get(). Blocks until Callable finishes.
         * 4. Callable completes, get() returns the exact String object instantiated
         *    by the worker thread.
         * ============================================================================
         */
        System.out.println("\n--- Awaiting Results ---");

        try {
            // Wait for Runnable (Returns null)
            Object rResult = runnableFuture.get(2, TimeUnit.SECONDS);
            System.out.println("Main Thread: Runnable Future get() returned -> " + rResult);

            // Wait for Callable (Returns actual data)
            String cResult = callableFuture.get(2, TimeUnit.SECONDS);
            System.out.println("Main Thread: Callable Future get() returned -> " + cResult);

        } catch (TimeoutException e) {
            System.err.println("Main Thread: A task timed out!");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Main Thread: Interrupted while waiting.");
        } catch (ExecutionException e) {
            // If the Callable threw an exception (e.g., SQLException), it is caught here.
            System.err.println("Main Thread: Task threw an exception: " + e.getCause());
        }

        // Graceful Shutdown
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.println("\n--- Execution Complete ---");
    }

    /**
     * Custom Thread Factory for proper debugging.
     */
    static class NamedThreadFactory implements ThreadFactory {
        private final AtomicInteger threadId = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "WorkerPool-Thread-" + threadId.getAndIncrement());
            t.setDaemon(false);
            return t;
        }
    }
}

/*
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity:
 *   Task submission is O(1) amortized. Fetching results via `Future.get()` is
 *   O(Wait Time), utilizing `LockSupport.park()` to suspend the calling thread
 *   with zero CPU spinning.
 *
 * - Space Complexity:
 *   O(T + Q) where T is the max pool size (4) and Q is the queue capacity (10).
 *   `Callable` adds a negligible O(1) auxiliary space overhead because it must be
 *   wrapped in a `FutureTask` object to hold the return state and outcome reference.
 *
 * - Thread Management Overhead:
 *   Both `Runnable` and `Callable` are executed by the same underlying worker
 *   threads (`ThreadPoolExecutor.Worker`). There is no performance penalty for
 *   choosing `Callable` over `Runnable`. The choice is strictly an architectural
 *   one based on whether a return value or propagated exception is required.
 * ============================================================================
 */