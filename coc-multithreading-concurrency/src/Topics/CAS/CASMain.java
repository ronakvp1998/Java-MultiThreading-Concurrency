package Topics.CAS;
/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Implement a thread-safe counter using lock-free mechanisms (Topics.CAS) and orchestrate
 * worker threads such that the main thread correctly awaits their completion before
 * reading the final state.
 *
 * Real-World Use Case:
 * High-performance, low-latency metrics collection (e.g., Prometheus counters,
 * statsd), rate limiters, or distributed sequence generators. In HFT (High-Frequency
 * Trading) platforms, maintaining lock-free counters for incoming tick data is
 * critical to avoid context-switching overhead caused by traditional locks.
 *
 * Concurrency Constraints:
 * 1. Atomicity: Increments must not be lost due to thread interleaving (Race Conditions).
 * 2. Visibility: Changes made by worker threads must be visible to the main reading thread.
 * 3. Orchestration: The reading thread must not read the value before writers finish.
 * ============================================================================
 */

import java.util.concurrent.atomic.AtomicInteger;

public class CASMain {

    /**
     * Shared Resource class utilizing java.util.concurrent.atomic.
     * We use AtomicInteger to guarantee atomic read-modify-write operations
     * without relying on expensive OS-level mutexes (synchronized blocks).
     */
    static class SharedResourceUsingCAS {
        // AtomicInteger uses unsafe.compareAndSwapInt under the hood.
//        final only locks the reference (the address).
//        It guarantees that the counter variable will always point to the exact same AtomicInteger object in memory.
//        It does not lock the data inside. The internal volatile int inside the object can still be updated by the incrementAndGet() method.
        private final AtomicInteger counter = new AtomicInteger(0);

        public void increment() {
            // Deep Dive: Why AtomicInteger over synchronized?
            // A synchronized method would suspend the thread if the lock is held,
            // resulting in a context switch (expensive in low-latency systems).
            // incrementAndGet() uses a do-while loop with a hardware-level
            // Compare-And-Swap (Topics.CAS) instruction. It loops until it successfully
            // applies the increment, maintaining the thread in user space.
            counter.incrementAndGet();
        }

        public int get() {
            // volatile read semantics guarantee we see the most recent write.
            return counter.get();
        }
    }

    public static void main(String[] args) {
        /*
         * ============================================================================
         * 3. IN-CODE DEEP DIVE & HAPPENS-BEFORE ANALYSIS
         * ============================================================================
         * Pitfall Avoided: The original code lacked thread orchestration. The main
         * thread spawned thread1 and thread2, then immediately executed System.out.println.
         * Because thread spawning is asynchronous, the main thread read '0' (or a low
         * random number) before the workers finished.
         *
         * Solution: We use thread.join().
         *
         * Happens-Before Relationship:
         * JLS Sec 17.4.5: All actions in a thread happen-before any other thread
         * successfully returns from a join() on that thread. Therefore, all Topics.CAS
         * increments in thread1 and thread2 are strictly guaranteed to be visible
         * to the main thread after the join() calls return.
         * ============================================================================
         */

        SharedResourceUsingCAS resource = new SharedResourceUsingCAS();

        Thread thread1 = new Thread(() -> {
            for (int i = 0; i < 200; i++) {
                resource.increment();
            }
        }, "WorkerThread-1");

        Thread thread2 = new Thread(() -> {
            for (int i = 0; i < 200; i++) {
                resource.increment();
            }
        }, "WorkerThread-2");

        /*
         * ============================================================================
         * 4. STEP-BY-STEP EXECUTION LOGIC
         * ============================================================================
         * 1. Main thread initializes SharedResourceUsingCAS (counter = 0).
         * 2. Main thread creates and starts WorkerThread-1 and WorkerThread-2.
         * 3. Both worker threads enter their loops and call increment().
         *    a. Under the hood, Thread-1 reads value 'X'.
         *    b. Thread-1 attempts to Topics.CAS 'X' to 'X+1'.
         *    c. If Thread-2 modified the value before Thread-1's Topics.CAS completes,
         *       Thread-1's Topics.CAS fails. Thread-1 re-reads the new value and retries.
         * 4. Main thread hits thread1.join() and blocks until WorkerThread-1 terminates.
         * 5. Main thread hits thread2.join() and blocks until WorkerThread-2 terminates.
         * 6. Both threads have terminated. Happens-before is established. Main thread
         *    safely reads the volatile state of AtomicInteger (exactly 400).
         * ============================================================================
         */

        thread1.start();
        thread2.start();

        try {
            // FIX: Main thread must wait for workers to finish to prevent premature reads.
            thread1.join();
            thread2.join();
        } catch (InterruptedException e) {
            // In production code, restore the interrupted status and handle gracefully
            Thread.currentThread().interrupt();
            System.err.println("Main thread interrupted while waiting for workers.");
        }

        // Guaranteed to be 400.
        System.out.println("Final counter value: " + resource.get());
    }
}

/*
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: O(N) per thread, where N is the number of loop iterations.
 *   However, in heavy contention, Topics.CAS operations degrade to O(N * retries) because
 *   failing threads must spin-wait. For just 2 threads, the collision rate is low.
 * - Space Complexity: O(1). The memory footprint is strictly the AtomicInteger object
 *   and the thread stack overhead.
 * - Synchronization Overhead:
 *   Topics.CAS is generally faster than intrinsic locks (synchronized) for low-to-medium
 *   contention because it avoids OS-level thread suspension and context switching.
 *   Under extreme contention (e.g., 100+ threads hitting the exact same memory address),
 *   Topics.CAS can suffer from "cache line bouncing" and spin-lock overhead. In such edge cases,
 *   modern Java (JDK 8+) recommends using `LongAdder` or `DoubleAdder`, which partition
 *   the counter across multiple variables to reduce cache invalidation traffic.
 * ============================================================================
 */