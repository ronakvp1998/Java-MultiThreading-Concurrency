/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement: 
 * Implement robust Inter-Thread Communication (ITC) where a Producer and a Consumer
 * thread must strictly alternate turns (ping-pong) modifying a shared resource.
 * The producer must wait if data is already present, and the consumer must wait
 * if no data is available.
 *
 * Real-World Use Case:
 * - Single-Slot Data Handoffs: A network acceptor thread receives a payload
 *   and must hand it directly to a single worker thread for processing before
 *   accepting the next payload.
 * - Synchronous Queues: The foundational logic behind `java.util.concurrent.SynchronousQueue`.
 *
 * Concurrency Constraints:
 * - Spurious Wakeups: Threads can wake up without a signal; condition checks
 *   MUST be in a `while` loop.
 * - Targeted Signaling: Waking up random threads (Thundering Herd) wastes CPU.
 *   Producers must specifically wake consumers, and vice-versa.
 */

package LocksAndConditions.InterthreadComm;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class InterThreadCommMain {

    public static void main(String[] args) {
        SharedResource resource = new SharedResource();

        Thread producer = new Thread(() -> {
            for (int i = 0; i < 5; i++) {
                resource.produce();
            }
        }, "Producer-Thread");

        Thread consumer = new Thread(() -> {
            for (int i = 0; i < 5; i++) {
                resource.consume();
            }
        }, "Consumer-Thread");

        producer.start();
        consumer.start();
    }
}

class SharedResource {

    private boolean isAvailable = false;
    private final ReentrantLock lock = new ReentrantLock();

    // SENIOR ARCHITECTURE: Two specific wait-queues tied to a single lock.
    // One for producers waiting for space, one for consumers waiting for data.
    private final Condition spaceAvailable = lock.newCondition();
    private final Condition dataAvailable = lock.newCondition();

    public void produce() {
        lock.lock();
        try {
            // 1. Guard against Spurious Wakeups using 'while'
            // Producer must wait if data is ALREADY available.
            while (isAvailable) {
                System.out.println("[-] Buffer full. " + Thread.currentThread().getName() + " is waiting...");
                spaceAvailable.await();
            }

            // 2. Critical Section Mutation
            System.out.println("[+] " + Thread.currentThread().getName() + " produced data.");
            isAvailable = true;

            // 3. TARGETED WAKEUP: Specifically wake up a Consumer.
            dataAvailable.signal();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Producer was interrupted.");
        } finally {
            lock.unlock();
        }
    }

    public void consume() {
        lock.lock();
        try {
            // 1. Guard against Spurious Wakeups using 'while'
            // Consumer must wait if data is NOT available (!isAvailable).
            while (!isAvailable) {
                System.out.println("[-] Buffer empty. " + Thread.currentThread().getName() + " is waiting...");
                dataAvailable.await();
            }

            // 2. Critical Section Mutation
            System.out.println("[-] " + Thread.currentThread().getName() + " consumed data.");
            isAvailable = false;

            // 3. TARGETED WAKEUP: Specifically wake up a Producer.
            spaceAvailable.signal();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Consumer was interrupted.");
        } finally {
            lock.unlock();
        }
    }

    // ============================================================================
    // 3. IN-CODE DEEP DIVE
    // ============================================================================
    /*
     * WHY REENTRANT-LOCK & CONDITIONS OVER SYNCHRONIZED/WAIT/NOTIFY?
     * `synchronized` blocks only allow a single implicit wait-set per object.
     * If you call `notifyAll()`, it wakes up every waiting thread, causing massive
     * CPU context-switching overhead (Thundering Herd). `ReentrantLock` allows
     * multiple `Condition` objects. `dataAvailable.signal()` wakes exactly one
     * consumer and ignores all waiting producers.
     *
     * HAPPENS-BEFORE RELATIONSHIPS:
     * When Producer calls `dataAvailable.signal()`, it transfers Consumer from the
     * condition queue to the lock queue. When Producer calls `unlock()`, the JMM
     * flushes `isAvailable = true` to main memory. When Consumer subsequently
     * re-acquires the lock to wake up from `await()`, it acts as a memory barrier,
     * guaranteeing it sees the updated state.
     *
     * PITFALLS (The "while" vs "if" debate):
     * The OS can wake a thread from `await()` due to internal thread scheduling,
     * signal interrupts, or false wakeups. If you use an `if` statement, the thread
     * proceeds blindly and overwrites data. A `while` loop forces the thread to
     * re-evaluate the business condition (`isAvailable`) and go back to sleep if
     * the wakeup was an error.
     */
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * 1. [Main] Starts both threads.
 * 2. [Producer] Acquires lock. `isAvailable` is false. Skips the `while` loop.
 * 3. [Producer] Prints "produced data", sets `isAvailable = true`.
 * 4. [Producer] Calls `dataAvailable.signal()`. (No effect if consumer hasn't started waiting).
 * 5. [Producer] Unlocks. Loops again instantly and re-acquires lock.
 * 6. [Producer] `isAvailable` is now true. Enters `while` loop, prints "Buffer full",
 *    and calls `spaceAvailable.await()`. Drops the lock and enters WAITING state.
 * 7. [Consumer] Acquires the dropped lock. `isAvailable` is true. Skips `while` loop.
 * 8. [Consumer] Prints "consumed data", sets `isAvailable = false`.
 * 9. [Consumer] Calls `spaceAvailable.signal()`. This moves Producer back to the ready queue.
 * 10. [Consumer] Unlocks. Loops again instantly.
 * 11. The threads continue this strict ping-pong exchange for all 5 iterations.
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: Lock acquisition and signaling are O(1) operations.
 * - Space Complexity: O(1) heap allocation for the AQS wait-nodes.
 * - Performance Overhead: Low latency. Eliminating `notifyAll()` prevents unnecessary
 *   CPU cycles spent waking threads that cannot proceed anyway.
 */