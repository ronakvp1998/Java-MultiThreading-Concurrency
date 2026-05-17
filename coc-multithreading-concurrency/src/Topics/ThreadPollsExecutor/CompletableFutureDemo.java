package Topics.ThreadPollsExecutor;

import java.util.concurrent.*;

/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * The provided code demonstrated a basic `CompletableFuture` submission but suffered
 * from two critical flaws:
 * 1. An Executor Resource Leak (the JVM would hang indefinitely because the custom
 *    ThreadPoolExecutor was never shut down).
 * 2. An anti-pattern of using the blocking `.get()` method immediately, which defeats
 *    the entire purpose of asynchronous, non-blocking pipelines.
 *
 * Real-World Use Case:
 * Microservice Aggregation Patterns (API Gateways). For instance, an e-commerce
 * checkout service needs to asynchronously fetch inventory status, process a payment,
 * and calculate shipping costs. Using `CompletableFuture` allows these network I/O
 * tasks to run concurrently and chain their results without blocking the container's
 * request threads (e.g., Tomcat's thread pool).
 *
 * Concurrency Constraints:
 * 1. Thread Starvation: Avoided by passing a custom, isolated `ThreadPoolExecutor`
 *    instead of relying entirely on the shared `ForkJoinPool.commonPool()`.
 * 2. Blocking Overheads: Overcome by utilizing callback chains (`thenApply`, `thenAccept`)
 *    instead of `Future.get()`.
 * 3. Resource Leaks: Worker threads must be explicitly terminated via `shutdown()`.
 * ============================================================================
 */
public class CompletableFutureDemo {

    public static void main(String[] args) {
        /*
         * ============================================================================
         * 3. IN-CODE DEEP DIVE: CONFIGURATION & HAPPENS-BEFORE
         * ============================================================================
         * Deep Dive: Custom Executor vs ForkJoinPool
         * By default, CompletableFuture uses ForkJoinPool.commonPool(). In a
         * high-throughput API, a single slow database query could exhaust the common
         * pool, starving all other unrelated asynchronous tasks. Injecting a custom
         * ThreadPoolExecutor implements the "Bulkhead Pattern," isolating this specific
         * workload.
         * ============================================================================
         */
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(
                1,                                      // corePoolSize
                1,                                      // maximumPoolSize
                3,                                      // keepAliveTime
                TimeUnit.HOURS,                         // unit
                new ArrayBlockingQueue<>(10),           // workQueue
                Executors.defaultThreadFactory(),       // threadFactory
                new ThreadPoolExecutor.AbortPolicy()    // rejection handler
        );

        try {
            System.out.println("--- Starting CompletableFuture Execution ---\n");

            /*
             * Happens-Before Relationship:
             * JLS Sec 17.4.5: The action of submitting the task (supplyAsync) happens-before
             * the worker thread begins execution. The completion of the 'supplyAsync'
             * stage happens-before the execution of the dependent 'thenApply' stage.
             */
            CompletableFuture<String> asyncTask1 = CompletableFuture.supplyAsync(() -> {
                String threadName = Thread.currentThread().getName();
                System.out.println("[" + threadName + "] Executing heavy network call...");
                try {
                    Thread.sleep(1000); // Simulate I/O bound work
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "Task completed";
            }, poolExecutor);

            /*
             * FIX: Demonstrating the Idiomatic Non-Blocking Pipeline.
             * Instead of immediately calling get(), we register callbacks. If the
             * asyncTask1 is already done, the main thread executes the callback.
             * If it is still running, the worker thread that finishes asyncTask1
             * will subsequently execute the callback.
             */
            CompletableFuture<Void> nonBlockingPipeline = asyncTask1
                    .thenApply(result -> {
                        // Data Transformation Stage
                        String threadName = Thread.currentThread().getName();
                        System.out.println("[" + threadName + "] Transforming result...");
                        return result + " with additional downstream processing";
                    })
                    .thenAccept(finalResult -> {
                        // Terminal Stage (Consumer)
                        String threadName = Thread.currentThread().getName();
                        System.out.println("[" + threadName + "] Final Output: " + finalResult);
                    })
                    .exceptionally(ex -> {
                        // Proper asynchronous exception handling
                        System.err.println("Pipeline failed: " + ex.getMessage());
                        return null;
                    });

            /*
             * ORIGINAL USER CODE (Retained for educational contrast).
             * Calling .get() blocks the main thread. We call it here at the end
             * strictly to prevent the main thread from exiting before our pipeline
             * completes, acting as a synchronization barrier.
             */
            System.out.println("\n[Main Thread] Doing other work while pipeline executes...");

            // Wait for both the original task and the pipeline to finish
            String blockingResult = asyncTask1.get();
            nonBlockingPipeline.get(); // Ensure the pipeline completes

            System.out.println("\n[Main Thread] Blocking Result retrieved: " + blockingResult);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Main thread interrupted.");
        } catch (ExecutionException e) {
            System.err.println("Task execution threw an exception: " + e.getCause());
        } finally {
            /*
             * FIX: The JVM will not terminate if non-daemon threads are alive.
             * Since corePoolSize is 1 and allowCoreThreadTimeOut is false by default,
             * the executor must be explicitly shut down.
             */
            System.out.println("\n[Main Thread] Initiating Graceful Shutdown...");
            poolExecutor.shutdown();
            try {
                if (!poolExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                    poolExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                poolExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        System.out.println("[Main Thread] Application Exited Safely.");
    }
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * 1. Main thread configures the ThreadPoolExecutor.
 * 2. Main thread calls `supplyAsync`. The `Runnable/Callable` is pushed to the
 *    ArrayBlockingQueue.
 * 3. The Executor spins up a worker thread ("pool-1-thread-1") which pulls the
 *    task from the queue and begins the 1-second sleep.
 * 4. Meanwhile, the Main thread registers `.thenApply`, `.thenAccept`, and
 *    `.exceptionally`. Because the task is not yet finished, these callbacks are
 *    pushed onto a lock-free Treiber Stack (Topics.CAS) inside the `CompletableFuture` instance.
 * 5. Main thread hits `asyncTask1.get()` and parks (suspends) via `LockSupport.park()`.
 * 6. Worker thread finishes the sleep, returns "Task completed".
 * 7. Worker thread pops the callbacks from the internal stack and executes
 *    `thenApply` and `thenAccept` sequentially on the same worker thread.
 * 8. Worker thread unparks the Main thread.
 * 9. Main thread resumes, retrieves the result, initiates shutdown, and exits.
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity:
 *   O(1) to construct the pipeline. The actual execution time is O(T) where T
 *   is the latency of the asynchronous operations.
 *
 * - Space Complexity:
 *   O(C) where C is the number of chained callbacks. Each `.then...` invocation
 *   creates a new `CompletableFuture` node (a Completion object) placed on the heap.
 *
 * - Synchronization Overhead:
 *   CompletableFuture entirely avoids traditional `synchronized` blocks. Under the
 *   hood, it relies heavily on `Unsafe`/`VarHandle` Compare-And-Swap (Topics.CAS)
 *   operations to update the `result` state and manage the stack of dependent
 *   actions. This guarantees extremely low contention even when thousands of
 *   threads are trying to register callbacks or complete the future simultaneously.
 * ============================================================================
 */