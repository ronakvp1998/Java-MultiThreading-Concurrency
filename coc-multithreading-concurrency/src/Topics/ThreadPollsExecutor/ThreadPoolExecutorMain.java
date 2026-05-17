package Topics.ThreadPollsExecutor;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Demonstrate the precise thread creation lifecycle, bounded queueing, and
 * rejection mechanisms of a custom Java ThreadPoolExecutor under a workload
 * that exceeds its maximum capacity.
 *
 * Real-World Use Case:
 * High-throughput web servers (e.g., Tomcat, Jetty), API rate limiters, or
 * asynchronous event processing systems (e.g., Kafka consumers). In these
 * systems, relying on unbounded queues (like Executors.newFixedThreadPool)
 * can lead to OutOfMemoryError (OOM) under massive load. A fully custom
 * ThreadPoolExecutor with a bounded queue and explicit rejection policy
 * ensures system stability and predictable degradation.
 *
 * Concurrency Constraints:
 * 1. Resource Exhaustion: Mitigated by using a bounded ArrayBlockingQueue.
 * 2. Thread Leakage: Handled by configuring keep-alive times and allowing core
 *    thread timeouts.
 * 3. Visibility/Ordering: The happens-before relationship between task submission
 *    and task execution ensures state visibility.
 * ============================================================================
 */
public class ThreadPoolExecutorMain {

    public static void main(String[] args) {
        /*
         * ============================================================================
         * 3. IN-CODE DEEP DIVE: CONFIGURATION & HAPPENS-BEFORE
         * ============================================================================
         * Deep Dive: Why custom ThreadPoolExecutor?
         * Using utility methods like Executors.newCachedThreadPool() creates
         * unbounded threads, leading to CPU thrashing. Executors.newFixedThreadPool()
         * uses an unbounded LinkedBlockingQueue, risking OOM. This custom setup
         * strictly bounds both threads and memory.
         *
         * Configuration Breakdown:
         * - corePoolSize (2): Minimum threads kept alive.
         * - maximumPoolSize (4): Maximum threads allowed.
         * - keepAliveTime (10 mins): Idle threads > core size are terminated after this.
         * - workQueue (ArrayBlockingQueue size 2): Strict bounded queue.
         *
         * Total Capacity = maximumPoolSize (4) + queueSize (2) = 6 concurrent tasks.
         * We are submitting 8 tasks, ensuring 2 will be rejected.
         *
         * Happens-Before Relationship:
         * JLS Sec 17.4.5: Actions in a thread prior to submitting a Runnable to an
         * Executor happen-before its execution begins. Any data prepared by the
         * main thread before executor.submit() is guaranteed visible to the worker.
         * ============================================================================
         */
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2,                                      // corePoolSize
                4,                                      // maximumPoolSize
                10,                                     // keepAliveTime
                TimeUnit.MINUTES,                       // unit
                new ArrayBlockingQueue<>(2),            // workQueue
                new CustomThreadFactory(),              // threadFactory
                new CustomRejectHandle()                // handler
        );

        // Allows even the core threads to terminate if idle for the keepAliveTime.
        // Useful for reclaiming system resources during absolute downtime.
        executor.allowCoreThreadTimeOut(true);

        /*
         * ============================================================================
         * 4. STEP-BY-STEP EXECUTION LOGIC
         * ============================================================================
         * Loop execution for i = 1 to 8:
         * 1. i=1: Pool size < Core (2). Creates Core Thread-1. Task executes.
         * 2. i=2: Pool size < Core (2). Creates Core Thread-2. Task executes.
         * 3. i=3: Pool size == Core. Adds Task 3 to ArrayBlockingQueue.
         * 4. i=4: Pool size == Core. Adds Task 4 to ArrayBlockingQueue. Queue is now FULL.
         * 5. i=5: Queue FULL. Pool size < Max (4). Creates Non-Core Thread-3. Task executes.
         * 6. i=6: Queue FULL. Pool size < Max (4). Creates Non-Core Thread-4. Task executes.
         * 7. i=7: Queue FULL AND Pool size == Max (4). Calls CustomRejectHandle.
         * 8. i=8: Queue FULL AND Pool size == Max (4). Calls CustomRejectHandle.
         * ============================================================================
         */
        for (int i = 1; i <= 8; i++) {
            final int taskId = i;
            executor.submit(() -> {
                try {
                    // FIX: Moved the print statement inside the worker thread logic.
                    // Previously, it was in the main thread, incorrectly printing "main".
                    System.out.println("Task " + taskId + " actively processed by " + Thread.currentThread().getName());
                    Thread.sleep(3000); // Simulate heavy I/O or CPU bound work
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Restore interrupt status
                    System.err.println("Task " + taskId + " interrupted.");
                }
            });
        }

        // Graceful Shutdown
        executor.shutdown();
        try {
            // Block main thread until all submitted tasks finish or timeout occurs
            if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
                executor.shutdownNow(); // Force cancel if timeout occurs
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.println("Main thread execution completed.");
    }
}

/**
 * Custom Handler for tasks that cannot be executed.
 * Pitfall Avoided: Default is AbortPolicy, which throws an unhandled
 * RejectedExecutionException, potentially crashing the submitting thread.
 */
class CustomRejectHandle implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        // In production, this might log to a monitoring system, persist the task
        // to a Dead Letter Queue (DLQ), or return a 429 Too Many Requests HTTP status.
        System.err.println("Task rejected by custom handler. Pool is completely exhausted.");
    }
}

/**
 * Custom Thread Factory to control thread metadata.
 * Crucial for debugging thread dumps in production.
 */
class CustomThreadFactory implements ThreadFactory {
    private final AtomicInteger threadId = new AtomicInteger(1);

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, "CustomWorkerThread-" + threadId.getAndIncrement());

        // Priority defaults to NORM_PRIORITY, explicitly setting for clarity.
        thread.setPriority(Thread.NORM_PRIORITY);

        // Setting to false ensures the JVM will not shut down until these threads finish.
        // If true (daemon), the JVM exits immediately when the main thread finishes.
        thread.setDaemon(false);

        return thread;
    }
}

/*
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity:
 *   Task submission via executor.submit() is O(1) amortized.
 *   The internal ArrayBlockingQueue uses a single ReentrantLock for both insertion
 *   and extraction, making enqueue operations fast but potentially contended
 *   under extreme microsecond-latency requirements.
 *
 * - Space Complexity:
 *   O(Q + T) where Q is the queue capacity (2) and T is the max threads (4).
 *   Strictly bounded memory footprint guarantees absolute OOM protection.
 *
 * - Synchronization Overhead:
 *   ThreadPoolExecutor utilizes a main `ReentrantLock` for updating pool state
 *   (workers set, pool size). However, task execution primarily relies on the
 *   concurrency primitives within the injected BlockingQueue. Using an
 *   `ArrayBlockingQueue` allocates array nodes up front (zero allocation during
 *   runtime), which reduces Garbage Collection (GC) pauses compared to
 *   `LinkedBlockingQueue`.
 * ============================================================================
 */