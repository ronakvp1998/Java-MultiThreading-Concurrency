package InterviewProblems.AlternatePrint;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ============================================================================
 * THE JAVA CONCURRENCY EXPERT: TIER-1 INTERVIEW PREP
 * ============================================================================
 *
 * 1. PROBLEM STATEMENT:
 * Coordinate exactly 2 threads to print a sequence of 3 distinct items (A, B, C)
 * in strict alternating order up to a defined maximum limit, explicitly using
 * explicit locking (ReentrantLock and Condition) instead of intrinsic monitors.
 *
 * 2. REAL-WORLD USE CASE:
 * High-Performance Event Loops / Bounded Queues. While `synchronized` is fine
 * for basic needs, Tier-1 systems (like Kafka or LMAX Disruptor concepts) often
 * require `ReentrantLock` for advanced capabilities: Fairness (preventing thread
 * starvation), interruptible lock acquisition (`lockInterruptibly()`), and the
 * ability to use multiple distinct `Condition` variables for highly granular
 * thread signaling (e.g., separate `notFull` and `notEmpty` queues).
 *
 * 3. CONCURRENCY CONSTRAINTS & CHALLENGES:
 * - Mutual Exclusion: Guarding the non-atomic `currentTurn` state.
 * - Lock Release Guarantees: Explicit locks MUST be released in a `finally`
 *   block. Failure to do so upon an exception guarantees a system-wide deadlock.
 * - Spurious Wakeups: Handled via `Condition.await()` inside a `while` loop.
 *
 * 4. STEP-BY-STEP EXECUTION LOGIC:
 * 1. Thread-0 calls `lock.lock()`. Acquires the lock.
 * 2. `currentTurn` is 0. `0 % 2 == 0` (matches Thread-0). Loop condition is false, proceeds.
 * 3. Thread-0 prints `LETTERS[0 % 3]` -> "A".
 * 4. Thread-0 increments `currentTurn` to 1.
 * 5. Thread-0 calls `condition.signal()`, moving Thread-1 from the condition wait-queue
 *    to the lock entry-queue.
 * 6. Thread-0 executes `finally { lock.unlock(); }`, actually releasing the lock.
 * 7. Thread-1 now acquires the lock, returns from `await()`, and evaluates its state.
 * 8. Cycle continues until `MAX_PRINTS` is reached.
 *
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS:
 * - Time/Space Complexity: Time is O(N) where N is `MAX_PRINTS`. Space is O(1).
 * - Performance Overhead: Uncontended `ReentrantLock` uses CAS (Compare-And-Swap)
 *   which is extremely fast. Contended locks park the thread at the OS level.
 *   Here, using `signal()` instead of `signalAll()` slightly optimizes performance
 *   by avoiding the "Thundering Herd" problem, though with only 2 threads, the
 *   difference is negligible.
 * ============================================================================
 */
public class AlternatingPrinterABC2Threads {

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        SharedPrinter printer = new SharedPrinter();

        // Submit Thread 0
        executor.submit(() -> printer.printSequence(0));

        // Submit Thread 1
        executor.submit(() -> printer.printSequence(1));

        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

class SharedPrinter {
    private int currentTurn = 0;
    private final int MAX_PRINTS = 15;
    private final String[] LETTERS = {"A", "B", "C"};

    /**
     * IN-CODE DEEP DIVE: WHY REENTRANTLOCK?
     * ReentrantLock provides API-level lock management rather than JVM-level.
     * We define it as `final` so the reference cannot be changed, preventing
     * threads from locking on different object monitors.
     */
    private final Lock lock = new ReentrantLock();

    // Condition variable tied directly to our specific lock instance.
    private final Condition condition = lock.newCondition();

    public void printSequence(int threadId) {
        while (currentTurn < MAX_PRINTS) {

            // 1. Acquire the lock explicitly.
            // Blocks current thread until the lock is available.
            lock.lock();
            try {
                // HAPPENS-BEFORE RELATIONSHIP:
                // The `lock.unlock()` by the previous thread happens-before
                // this `lock.lock()` returns. This guarantees the current thread
                // sees the most up-to-date value of `currentTurn`.

                while (currentTurn % 2 != threadId && currentTurn < MAX_PRINTS) {
                    // PITFALL AVOIDANCE: await() vs wait()
                    // ReentrantLock uses await(), not wait(). Calling wait() on a
                    // Condition object throws IllegalMonitorStateException.
                    // await() atomically releases the lock and suspends the thread.
                    condition.await();
                }

                // Double check condition after returning from await()
                if (currentTurn >= MAX_PRINTS) {
                    // Signal any waiting thread so it doesn't block forever on shutdown
                    condition.signalAll();
                    break;
                }

                System.out.println(LETTERS[currentTurn % 3] + " - " + Thread.currentThread().getName());

                currentTurn++;

                // 2. Signal the other thread.
                // We use signal() instead of signalAll() here because we know exactly
                // one other thread is waiting. This is slightly more efficient.
                condition.signal();

            } catch (InterruptedException e) {
                // Restore interrupt status and exit gracefully
                Thread.currentThread().interrupt();
                return;
            } finally {
                // PITFALL AVOIDANCE: The Deadlock Trap
                // unlock() MUST be in a finally block. If `System.out.println`
                // throws an OutOfMemoryError, or thread is interrupted, the lock
                // is safely released for other threads.
                lock.unlock();
            }
        }
    }
}

//package InterviewProblems.AlternatePrint;
//
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.TimeUnit;
//
///**
// * ============================================================================
// * THE JAVA CONCURRENCY EXPERT: TIER-1 INTERVIEW PREP
// * ============================================================================
// *
// * 1. PROBLEM STATEMENT:
// * Coordinate exactly 2 threads to print a sequence of 3 distinct items (A, B, C)
// * in strict alternating order up to a defined maximum limit.
// *
// * 2. REAL-WORLD USE CASE:
// * Hardware / Resource Multiplexing. This pattern models a scenario where the
// * number of available worker threads (workers = 2) does not perfectly map to the
// * number of distinct tasks or states in a state machine (states = 3). It mimics
// * Round-Robin scheduling or handling state transitions in networking protocols
// * across a constrained, bounded thread pool.
// *
// * 3. CONCURRENCY CONSTRAINTS & CHALLENGES:
// * - Mutual Exclusion: Both threads mutate the shared state (`currentTurn`).
// * - Visibility: Changes to `currentTurn` by one thread must be immediately
// *   visible to the other.
// * - Spurious Wakeups: Threads waking up without a signal must be handled.
// * - Thread Coordination: Strict alternation requires deterministic waiting and
// *   signaling rather than relying on OS-level thread scheduling.
// *
// * 4. STEP-BY-STEP EXECUTION LOGIC:
// * 1. Thread-0 acquires the lock. `currentTurn` is 0.
// *    `0 % 2 == 0` (matches Thread-0). Thread-0 proceeds.
// * 2. Thread-0 prints `LETTERS[0 % 3]` -> "A".
// * 3. Thread-0 increments `currentTurn` to 1, calls `notifyAll()`, releases lock.
// * 4. Thread-1 acquires the lock. `currentTurn` is 1.
// *    `1 % 2 == 1` (matches Thread-1). Thread-1 proceeds.
// * 5. Thread-1 prints `LETTERS[1 % 3]` -> "B".
// * 6. Thread-1 increments `currentTurn` to 2, calls `notifyAll()`, releases lock.
// * 7. Thread-0 acquires the lock. `currentTurn` is 2.
// *    `2 % 2 == 0` (matches Thread-0). Thread-0 proceeds.
// * 8. Thread-0 prints `LETTERS[2 % 3]` -> "C".
// * 9. Cycle continues until `currentTurn` reaches `MAX_PRINTS`.
// *
// * 5. COMPLEXITY & PERFORMANCE ANALYSIS:
// * - Time Complexity: O(N) where N is `MAX_PRINTS`. Each thread does O(1) work per turn.
// * - Space Complexity: O(1) ignoring the fixed thread stack memory overhead.
// * - Performance Overhead: Using `synchronized` combined with `wait()/notifyAll()`
// *   causes context switching via the OS scheduler. In a 2-thread scenario, the
// *   "Thundering Herd" problem caused by `notifyAll()` is effectively nullified
// *   (only 1 thread is ever waiting). However, for ultra-low latency, busy-spinning
// *   or `LockSupport.park()` with explicit unparking would reduce latency at the
// *   cost of CPU cycles.
// * ============================================================================
// */
//public class AlternatingPrinterABC2Threads {
//
//    public static void main(String[] args) {
//        // We utilize ExecutorService to manage thread lifecycle rather than raw Threads.
//        // newFixedThreadPool(2) ensures exactly 2 worker threads handle our tasks.
//        ExecutorService executor = Executors.newFixedThreadPool(2);
//        SharedPrinter printer = new SharedPrinter();
//
//        // Submit Thread 0
//        executor.submit(() -> printer.printSequence(0));
//
//        // Submit Thread 1
//        executor.submit(() -> printer.printSequence(1));
//
//        // Graceful shutdown sequence: stop accepting tasks, await current tasks.
//        executor.shutdown();
//        try {
//            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
//                executor.shutdownNow();
//            }
//        } catch (InterruptedException e) {
//            executor.shutdownNow();
//            Thread.currentThread().interrupt();
//        }
//    }
//}
//
//class SharedPrinter {
//    // Shared state. Does NOT need `volatile` because read/write access
//    // is heavily guarded by the intrinsic lock (synchronized block).
//    private int currentTurn = 0;
//    private final int MAX_PRINTS = 15;
//    private final String[] LETTERS = {"A", "B", "C"};
//
//    /**
//     * IN-CODE DEEP DIVE: WHY SYNCHRONIZED?
//     * While ReentrantLock/Condition are superior for multi-condition waits,
//     * intrinsic locks (synchronized) are perfectly adequate and idiomatic
//     * for simple binary alternation. The JVM automatically handles the lock
//     * acquisition and release, reducing boilerplate.
//     *
//     * HAPPENS-BEFORE RELATIONSHIP:
//     * 1. The release of the intrinsic lock by Thread A (via wait() or exiting method)
//     *    happens-before the subsequent acquisition of the same lock by Thread B.
//     * 2. This guarantees Thread B sees the updated `currentTurn` value.
//     */
//    public synchronized void printSequence(int threadId) {
//        while (currentTurn < MAX_PRINTS) {
//
//            // PITFALL AVOIDANCE: Spurious Wakeups
//            // Always await conditions inside a while loop. If the OS wakes the thread
//            // spuriously, it re-evaluates the condition and goes back to sleep if
//            // it is not its turn.
//            // Logic: `currentTurn % 2` maps the monotonically increasing counter
//            // strictly to Thread 0 or Thread 1.
//            while (currentTurn % 2 != threadId && currentTurn < MAX_PRINTS) {
//                try {
//                    // Releases the intrinsic lock and suspends the thread.
//                    wait();
//                } catch (InterruptedException e) {
//                    // PITFALL AVOIDANCE: Swallowed Interrupts
//                    // Restore the interrupt flag so higher-level managers (like Executor)
//                    // can handle the thread shutdown correctly.
//                    Thread.currentThread().interrupt();
//                    return;
//                }
//            }
//
//            // PITFALL AVOIDANCE: Post-Wakeup Bounds Check
//            // A thread could wake up after the other thread reached MAX_PRINTS.
//            // We must double-check the bounds before executing the critical logic.
//            if (currentTurn >= MAX_PRINTS) {
//                // Before breaking, notify the other thread in case it is waiting,
//                // preventing indefinite blocking during shutdown.
//                notifyAll();
//                break;
//            }
//
//            // Logic: `currentTurn % 3` maps the counter to indices 0, 1, 2 (A, B, C)
//            System.out.println(LETTERS[currentTurn % 3] + " - " + Thread.currentThread().getName());
//
//            // State mutation
//            currentTurn++;
//
//            // Signal all waiting threads (which is just the 1 other thread in this case)
//            // that the state has changed.
//            notifyAll();
//        }
//    }
//}