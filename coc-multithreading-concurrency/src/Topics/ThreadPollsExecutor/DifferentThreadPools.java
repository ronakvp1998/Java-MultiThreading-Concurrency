package Topics.ThreadPollsExecutor;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ============================================================================
 * PROBLEM STATEMENT:
 * The application requires managing concurrent task execution efficiently using
 * various thread pool configurations. Unbounded thread creation leads to CPU
 * thrashing and memory exhaustion (OutOfMemoryError). The goal is to understand
 * and properly implement different `ExecutorService` patterns provided by the JVM.
 *
 * REAL-WORLD USE CASE:
 * 1. CustomThreadPool: High-throughput transaction processing systems (e.g., HFT
 *    order gateways) where precise control over queue limits and rejection is required.
 * 2. FixedThreadPool: Web servers (like Tomcat) or database connection pools handling
 *    a predictable, steady load of concurrent requests.
 * 3. CachedThreadPool: Short-lived, asynchronous burst tasks (e.g., sending parallel
 *    non-blocking I/O network requests, like analytics pingbacks).
 * 4. SingleThreadExecutor: Sequential event processing, logging daemons, or maintaining
 *    strict ordering of state-changing events (e.g., matching engine sequential core).
 *
 * CONCURRENCY CONSTRAINTS:
 * - Thread Starvation: Long-running tasks occupying all pool threads, blocking new ones.
 * - Memory Exhaustion: `Executors.newFixedThreadPool` uses an unbounded `LinkedBlockingQueue`.
 *   If producers outpace consumers, the queue grows infinitely.
 * - Visibility & Ordering: Ensuring that variables modified before task submission
 *   are visible to the worker thread executing the task.
 * ============================================================================
 */
public class DifferentThreadPools {

    public static void main(String[] args) {
        /*
         * --------------------------------------------------------------------
         * 1. CUSTOM THREAD POOL EXECUTOR
         * --------------------------------------------------------------------
         * WHY: Provides maximum control. Defines core threads, max threads,
         * keep-alive time, queue capacity, thread naming (via factory), and
         * backpressure strategy (via rejection handler).
         */
        ThreadPoolExecutor customPoolExecutor = new ThreadPoolExecutor(
                2,                               // corePoolSize: Keep 2 threads alive even if idle
                5,                               // maximumPoolSize: Max 5 threads during spikes
                1, TimeUnit.HOURS,               // keepAliveTime: Kill excess threads (>2) after 1 hr idle
                new ArrayBlockingQueue<>(10),    // workQueue: Bounded queue! Prevents OOM. Max 10 waiting tasks.
                new CustomThreadFactory(),       // threadFactory: For custom naming/daemon status
                new CustomRejectHandle()         // handler: Strategy when max threads + queue are full
        );

        /*
         * --------------------------------------------------------------------
         * 2. FIXED THREAD POOL
         * --------------------------------------------------------------------
         * WHY: Caps the number of active threads to a specific number (5).
         * PITFALL: Internally uses `new LinkedBlockingQueue<Runnable>()` which is UNBOUNDED.
         * If tasks arrive faster than 5 threads can process them, memory will eventually exhaust.
         */
        ExecutorService fixPoolExecutor = Executors.newFixedThreadPool(5);

        /*
         * --------------------------------------------------------------------
         * 3. CACHED THREAD POOL
         * --------------------------------------------------------------------
         * WHY: Good for many short-lived tasks. Scales threads infinitely.
         * PITFALL: maximumPoolSize is Integer.MAX_VALUE. Can lead to thread exhaustion
         * (unable to create new native thread) if tasks block or take too long.
         * Uses `SynchronousQueue` (capacity of 0) - forces immediate handoff to a thread.
         */
        ExecutorService cachedPoolExecutor = Executors.newCachedThreadPool();

        /*
         * --------------------------------------------------------------------
         * 4. SINGLE THREAD EXECUTOR
         * --------------------------------------------------------------------
         * WHY: Guarantees sequential execution of tasks.
         * PITFALL: Like FixedThreadPool, uses an unbounded queue.
         * Note: Cannot be reconfigured at runtime unlike a FixedThreadPool(1).
         */
        ExecutorService singlePoolExecutor = Executors.newSingleThreadExecutor();

        /*
         * STEP-BY-STEP EXECUTION LOGIC (Simulating workload):
         * 1. Main thread iterates 20 times, creating 20 Runnable tasks.
         * 2. Task submission (execute) establishes a Happens-Before relationship.
         *    Any memory written by the main thread BEFORE execute() is guaranteed
         *    visible to the worker thread executing the task.
         * 3. CustomPool Logic:
         *    - Tasks 1-2 go to Core Threads.
         *    - Tasks 3-12 go to the ArrayBlockingQueue.
         *    - Tasks 13-15 trigger Max Threads (spins up threads 3, 4, 5).
         *    - Tasks 16-20 are REJECTED (handled by CustomRejectHandle) because Queue+Max is full.
         */
        System.out.println("--- Submitting tasks to CustomThreadPool ---");
        for (int i = 1; i <= 20; i++) {
            final int taskId = i;
            customPoolExecutor.execute(() -> simulateWork(taskId));
        }

        // Graceful Shutdown Sequence
        shutdownAndAwaitTermination(customPoolExecutor, "CustomPool");
        shutdownAndAwaitTermination(fixPoolExecutor, "FixedPool");
        shutdownAndAwaitTermination(cachedPoolExecutor, "CachedPool");
        shutdownAndAwaitTermination(singlePoolExecutor, "SinglePool");
    }

    /**
     * Simulates IO/CPU work to demonstrate thread utilization.
     */
    private static void simulateWork(int taskId) {
        try {
            System.out.println(Thread.currentThread().getName() + " is processing Task-" + taskId);
            Thread.sleep(500); // Simulate task taking 500ms
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt status
            System.err.println("Task-" + taskId + " was interrupted.");
        }
    }

    /**
     * Boilerplate for safe, graceful shutdown of executors.
     */
    private static void shutdownAndAwaitTermination(ExecutorService pool, String poolName) {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(2, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(2, TimeUnit.SECONDS)) {
                    System.err.println(poolName + " did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            Thread.currentThread().interrupt(); // Preserve interrupt status
        }
    }

    /*
     * ========================================================================
     * CUSTOM COMPONENTS & OVERRIDES
     * ========================================================================
     */

    /**
     * Custom ThreadFactory.
     * WHY: Essential for production debugging. Default threads are named
     * "pool-1-thread-1", which is useless in stack traces. This provides
     * meaningful business contexts to thread names and can set daemon status.
     */
    static class CustomThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix = "TradeProcessor-Thread-";

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
            t.setPriority(Thread.NORM_PRIORITY);
            t.setDaemon(false); // JVM will not exit until these threads finish
            return t;
        }
    }

    /**
     * Custom RejectedExecutionHandler.
     * WHY: When the bounded queue is full AND all max threads are busy,
     * new tasks MUST be rejected to prevent system collapse.
     * Strategies include: AbortPolicy (throw exception), CallerRunsPolicy
     * (slow down producer), DiscardPolicy, or Custom (log and metrics).
     */
    static class CustomRejectHandle implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            // In a real system, you'd increment a Micrometer/Prometheus metric here.
            System.err.println("WARNING: Task rejected. Pool is fully saturated. Active Threads: "
                    + executor.getActiveCount() + ", Queue Size: " + executor.getQueue().size());
        }
    }
}

/*
 * ============================================================================
 * COMPLEXITY & PERFORMANCE ANALYSIS:
 * ============================================================================
 * Space Complexity:
 * - Fixed/Single pools: O(T) where T is the number of submitted tasks (due to
 *   unbounded LinkedBlockingQueue). Risk of OOM.
 * - Custom pool: O(N) where N is queue capacity (10). Memory footprint is strictly bounded.
 *
 * Time Complexity (Task Submission):
 * - O(1) for adding to ArrayBlockingQueue/LinkedBlockingQueue.
 *
 * Synchronization Overhead:
 * - ThreadPoolExecutor relies heavily on `ReentrantLock` (mainLock) to update
 *   pool state (core size, max size) and `Condition` variables for workers waiting
 *   on the queue (`queue.take()`).
 * - `ArrayBlockingQueue` uses a single lock for both put/take, which can cause
 *   slight contention under massive concurrency compared to `LinkedBlockingQueue`
 *   (which uses two-lock queue algorithm: putLock and takeLock). However, for
 *   predictable latency and avoiding GC pauses (no node allocations),
 *   ArrayBlockingQueue is strictly preferred in HFT/low-latency domains.
 * ============================================================================
 */