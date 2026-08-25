package InterviewProblems;

/**
 * ============================================================================
 * 1. HEADER & PROBLEM CONTEXT
 * ============================================================================
 * Problem Statement: Custom ThreadPool Executor from Scratch
 *
 * Design and implement a custom ThreadPool Executor in Java without using the
 * built-in java.util.concurrent.ExecutorService or ThreadPoolExecutor classes.
 *
 * The custom thread pool must:
 * 1. Initialize with a fixed number of worker threads.
 * 2. Accept tasks (Runnable) via an execute() method.
 * 3. Buffer tasks if all worker threads are busy.
 * 4. Provide a graceful shutdown() method that prevents new tasks from being
 *    submitted but completes all currently queued tasks before terminating threads.
 *
 * Constraints:
 * - Thread pool size > 0.
 * - Thread-safe task submission and execution.
 * - No memory leaks (threads must eventually die after shutdown is called and
 *   queue is empty).
 *
 * Input/Output Formats:
 * Input: Submit multiple Runnable tasks concurrently.
 * Output: Console logs demonstrating tasks being picked up and executed by a
 * bounded number of worker threads, followed by a clean shutdown.
 * ============================================================================
 */

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ThreadPoolMasterclass {

    /**
     * ============================================================================
     * 2.2 PROGRESSIVE IMPLEMENTATION ROADMAP (Non-DP)
     * ============================================================================
     * Phase 1: Optimal Approach - Custom Fixed Thread Pool
     *
     * Intuition:
     * A ThreadPool is fundamentally the Producer-Consumer pattern.
     * - The "Producers" are the threads calling `execute(task)`.
     * - The "Buffer" is a thread-safe BlockingQueue.
     * - The "Consumers" are a fixed array of pre-instantiated Worker threads
     *   that infinitely loop, pulling tasks from the queue and calling `run()`.
     *
     * To handle shutdowns gracefully, we use a `volatile boolean` flag. When
     * shutdown is called, we reject new tasks and interrupt the worker threads.
     * The workers catch the interruption, check if the queue is empty, and if so,
     * break their infinite loop, allowing the threads to terminate naturally.
     *
     * Complexity Analysis:
     * - Time Complexity: O(1) for task submission. Task execution depends on the task.
     * - Space Complexity: O(N + Q) where N is the number of threads and Q is the
     *   maximum number of tasks in the blocking queue.
     * ============================================================================
     */
    static class CustomThreadPool {
        private final BlockingQueue<Runnable> taskQueue;
        private final WorkerThread[] workers;
        private volatile boolean isShutdown;

        public CustomThreadPool(int numThreads) {
            this.taskQueue = new LinkedBlockingQueue<>();
            this.workers = new WorkerThread[numThreads];
            this.isShutdown = false;

            // Pre-start all worker threads
            for (int i = 0; i < numThreads; i++) {
                workers[i] = new WorkerThread("CustomWorker-" + (i + 1));
                workers[i].start();
            }
        }

        // Submits a task to the queue
        public void execute(Runnable task) {
            if (isShutdown) {
                throw new IllegalStateException("ThreadPool is shut down. Cannot accept new tasks.");
            }
            try {
                taskQueue.put(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Task submission interrupted.");
            }
        }

        // Initiates a graceful shutdown
        public void shutdown() {
            System.out.println("Initiating ThreadPool Shutdown...");
            isShutdown = true;
            // Interrupt all workers to wake them up if they are blocked waiting for tasks
            for (WorkerThread worker : workers) {
                worker.interrupt();
            }
        }

        // Inner class representing the continuously running worker threads
        private class WorkerThread extends Thread {
            public WorkerThread(String name) {
                super(name);
            }

            @Override
            public void run() {
                // Loop until the pool is shut down AND there are no tasks left
                while (!isShutdown || !taskQueue.isEmpty()) {
                    try {
                        // take() blocks until a task is available.
                        // If interrupted during take(), it throws InterruptedException.
                        Runnable task = taskQueue.take();

                        // Execute the task synchronously in this worker thread
                        task.run();

                    } catch (InterruptedException e) {
                        // Thread was interrupted during shutdown.
                        // The loop condition will re-evaluate on the next iteration.
                        // If shutdown is true and queue is empty, the loop breaks.
                    } catch (RuntimeException e) {
                        // Catch application exceptions so one bad task doesn't kill the worker thread
                        System.err.println("Task threw an exception: " + e.getMessage());
                    }
                }
                System.out.println(Thread.currentThread().getName() + " has terminated.");
            }
        }
    }

    /**
     * ============================================================================
     * Phase 2: Brute Force Approach - Thread-Per-Task Execution
     *
     * Intuition:
     * The brute-force alternative to a ThreadPool is spawning a brand-new OS-level
     * thread for every single task.
     *
     * Why is this bad?
     * 1. Thread creation and destruction is expensive at the OS level.
     * 2. If 10,000 tasks arrive simultaneously, it will spawn 10,000 threads,
     *    likely causing an OutOfMemoryError or severe CPU context-switching thrashing.
     *
     * Complexity Analysis:
     * - Time Complexity: O(1) submission, but massive CPU overhead for thread creation.
     * - Space Complexity: O(T) where T is the number of active tasks, potentially
     *   exhausting RAM.
     * ============================================================================
     */
    static class ThreadPerTaskExecutor {
        public void execute(Runnable task) {
            // Creates a new thread for EVERY task - HIGHLY INEFFICIENT
            new Thread(task).start();
        }
    }

    /**
     * ============================================================================
     * Phase 3: Alternative Approaches
     *
     * 1. java.util.concurrent.ThreadPoolExecutor: The production standard. Includes
     *    core pool size, max pool size, keep-alive times, and rejection handlers.
     * 2. ForkJoinPool: Optimized for divide-and-conquer algorithms (work-stealing algorithm).
     * 3. ScheduledThreadPoolExecutor: Optimized for executing tasks periodically or
     *    after a delay.
     * 4. Virtual Threads (Java 21+): Makes the "Thread-Per-Task" model viable again
     *    by using lightweight, JVM-managed threads instead of OS threads.
     * ============================================================================
     */

    /**
     * ============================================================================
     * 4. TESTING SUITE
     * ============================================================================
     */
    public static void main(String[] args) throws InterruptedException {
        System.out.println("--- Starting Phase 1: Custom ThreadPool Test ---");

        // Create a pool with only 3 worker threads
        CustomThreadPool threadPool = new CustomThreadPool(3);

        // Submit 10 tasks to the pool
        for (int i = 1; i <= 10; i++) {
            final int taskId = i;
            threadPool.execute(() -> {
                System.out.println(Thread.currentThread().getName() + " is executing Task-" + taskId);
                try {
                    // Simulate processing time
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // Give the pool a moment to process some tasks
        Thread.sleep(100);

        // Shut down the pool.
        // Notice that tasks already in the queue will still finish.
        threadPool.shutdown();

        // Attempting to add a new task after shutdown should fail
        try {
            threadPool.execute(() -> System.out.println("This should not run"));
        } catch (IllegalStateException e) {
            System.out.println("\nExpected Exception Caught: " + e.getMessage() + "\n");
        }

        // Wait for workers to finish remaining queue items and terminate
        Thread.sleep(1000);
        System.out.println("Test Complete. Main thread exiting.");
    }
}