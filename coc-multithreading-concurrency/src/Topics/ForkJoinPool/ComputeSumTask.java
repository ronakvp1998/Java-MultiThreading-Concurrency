package Topics.ForkJoinPool;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveTask;

/**
 * ============================================================================
 * PROBLEM STATEMENT:
 * Compute the sum of a massive range of integers (or elements in an array)
 * using a parallel divide-and-conquer strategy. The workload must be dynamically
 * balanced across available CPU cores to minimize execution time.
 *
 * REAL-WORLD USE CASE:
 * 1. Big Data Aggregation: Foundational to Java's `Stream.parallel().reduce()`
 *    and `Arrays.parallelSort()`.
 * 2. Financial Modeling: Parallel computation of Monte Carlo simulation paths
 *    where intermediate results must be aggregated (MapReduce paradigm).
 * 3. Image Processing: Applying transformations to large pixel matrices where
 *    the matrix can be recursively split into quadrants.
 *
 * CONCURRENCY CONSTRAINTS:
 * - Thread Starvation: If a thread blocks waiting for subtasks to complete without
 *   contributing to the computation, the pool can run out of active threads.
 * - Work-Stealing Overhead: If the base-case threshold is too small, the cost of
 *   object allocation (`ComputeSumTask`) and queue management will eclipse the
 *   actual computation time.
 * - Visibility: Ensuring the aggregated sums from subtasks are safely published
 *   and visible to the parent task without explicit locking.
 * ============================================================================
 */
public class ComputeSumTask extends RecursiveTask<Integer> {

    // Threshold determines when to stop splitting and compute sequentially.
    // In production, this should be tuned based on the workload (e.g., 10,000+ ops).
    private static final int THRESHOLD = 4;

    private final int start;
    private final int end;

    public ComputeSumTask(int start, int end){
        this.start = start;
        this.end = end;
    }

    @Override
    protected Integer compute() {
        /*
         * STEP-BY-STEP EXECUTION LOGIC:
         * 1. Check if the current range (end - start) is within the THRESHOLD.
         * 2. If YES (Base Case): Execute the summation sequentially in the current thread.
         * 3. If NO (Recursive Step): Calculate the midpoint to divide the work in half.
         * 4. Instantiate `leftTask` and `rightTask`.
         * 5. `fork()` the left task, pushing it to the thread's local deque to be
         *    stolen by an idle thread.
         * 6. `compute()` the right task IMMEDIATELY in the current thread (optimizing CPU usage).
         * 7. `join()` the left task, blocking until the stolen (or locally executed)
         *    left subtask completes.
         * 8. Aggregate and return the sum.
         */
        if (end - start <= THRESHOLD) {
            int totalSum = 0;
            for (int i = start; i <= end; i++) {
                totalSum += i;
            }
            return totalSum;
        } else {
            int mid = start + (end - start) / 2; // Prevents integer overflow vs (start+end)/2
            ComputeSumTask leftTask = new ComputeSumTask(start, mid);
            ComputeSumTask rightTask = new ComputeSumTask(mid + 1, end);

            /*
             * ----------------------------------------------------------------
             * CODE CORRECTION & DEEP DIVE: The "Double Fork" Anti-Pattern
             * ----------------------------------------------------------------
             * ORIGINAL CODE:
             * leftTask.fork();
             * rightTask.fork();
             *
             * WHY IT WAS WRONG: Calling fork() on both tasks pushes both to the
             * queue and leaves the current thread idle, waiting for both to finish.
             * This wastes a thread and defeats the purpose of work-stealing.
             *
             * CORRECT APPROACH:
             * 1. fork() the left task (pushes to head of local deque).
             * 2. compute() the right task directly in the current thread.
             * 3. join() the left task.
             * (Alternatively, `invokeAll(leftTask, rightTask)` handles this internally).
             */

            // Push left task to local deque. It may be stolen by another thread.
            leftTask.fork();

            // Compute right task locally. Current thread does actual work instead of waiting.
            int rightResults = rightTask.compute();

            /*
             * HAPPENS-BEFORE RELATIONSHIP:
             * The completion of `leftTask` (the return of its compute() method)
             * HAPPENS-BEFORE the `join()` method returns. This guarantees that
             * the `leftResults` memory is fully visible to the current thread.
             */
            int leftResults = leftTask.join();

            return leftResults + rightResults;
        }
    }

    public static void main(String[] args) {
        /*
         * WHY ForkJoinPool.commonPool()?
         * The commonPool is statically constructed and shared across the JVM.
         * It is highly optimized and automatically sizes itself to
         * Runtime.getRuntime().availableProcessors() - 1.
         * Note: Never execute blocking I/O tasks in the commonPool, as it will
         * paralyze parallel streams and other FJP tasks JVM-wide.
         */
        ForkJoinPool pool = ForkJoinPool.commonPool();

        // Note: start is 0, end is 100. Range size is 101 elements.
        Future<Integer> futureObj = pool.submit(new ComputeSumTask(0, 100));

        try {
            // .get() blocks the main thread until the computation graph completes.
            System.out.println("Total Sum (0 to 100): " + futureObj.get());
        } catch (Exception e) {
            // In production, handle InterruptedException and ExecutionException appropriately.
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
    }
}

/*
 * ============================================================================
 * COMPLEXITY & PERFORMANCE ANALYSIS:
 * ============================================================================
 * Time Complexity:
 * - Total Work: $O(N)$ where $N$ is the range of numbers.
 * - Span (Parallel Execution Time): $O(\log N)$ assuming infinite processors,
 *   due to the depth of the binary recursion tree.
 * - Real-world Time Complexity: $O(N / P + \log P)$ where $P$ is the number
 *   of active CPU cores.
 *
 * Space Complexity:
 * - Call Stack: $O(\log N)$ due to the recursive splitting.
 * - Object Allocation: $O(N / \text{THRESHOLD})$ task objects are instantiated
 *   on the heap. If the threshold is too small, Garbage Collection (GC) pauses
 *   will negate any parallelization benefits.
 *
 * Synchronization Overhead:
 * - Minimal. The Work-Stealing queue uses a Lock-Free, Double-Ended Queue (Deque).
 *   The owner thread pushes/pops from the HEAD using atomic operations, while
 *   idle threads "steal" from the TAIL using Compare-And-Swap (CAS). This drastically
 *   reduces lock contention compared to a traditional `ThreadPoolExecutor` using a
 *   shared `BlockingQueue`.
 * ============================================================================
 */