/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement: 
 * Design a robust, thread-safe rate limiter that restricts the maximum number 
 * of concurrent executions for a specific heavily-loaded operation. When the 
 * system reaches the maximum allowed concurrency, subsequent threads must either 
 * queue up (block) or immediately abort (fail-fast) to prevent cascading system 
 * failures and resource exhaustion.
 *
 * Real-World Use Case: 
 * - API Gateway Throttling: Restricting outbound calls to a legacy 3rd-party API 
 *   that crashes if it receives more than 5 concurrent requests.
 * - Database Connection Pooling: Ensuring only a fixed number of application 
 *   threads can lease a physical TCP connection to the database at any given time.
 *
 * Concurrency Constraints: 
 * - Bounded Concurrency: Unlike a ReentrantLock (which enforces 1 thread at a time), 
 *   this requires exactly N threads to execute simultaneously.
 * - Starvation Prevention: Highly contested throttlers must grant access fairly 
 *   (FIFO) to prevent older requests from timing out while newer ones succeed.
 * - Leak Prevention: If a thread crashes or times out mid-execution, its permit 
 *   MUST be returned to the pool, or the system will permanently shrink capacity.
 */

package Topics.LocksAndConditions.SemaphoreLock;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BoundedApiRateLimiterDemo {

    /**
     * Shared Resource representing our API Gateway.
     */
    static class PaymentApiGateway {

        private final int MAX_CONCURRENT_REQUESTS = 3;

        // SENIOR UPGRADE: 
        // 1. We initialize with 3 permits.
        // 2. We pass 'true' for fairness. This forces the underlying AbstractQueuedSynchronizer 
        //    (AQS) to grant permits in strict FIFO order, preventing thread starvation 
        //    under massive load.
        private final Semaphore throttle = new Semaphore(MAX_CONCURRENT_REQUESTS, true);

        // Just for metrics tracking in our demo
        private final AtomicInteger activeRequests = new AtomicInteger(0);

        /**
         * Processes a payment, respecting the bounded concurrency limit.
         */
        public void processPayment(String transactionId) {
            System.out.println("[" + Thread.currentThread().getName() + "] Arrived at Gateway for " + transactionId + ". Attempting to acquire permit...");

            try {
                // SENIOR UPGRADE: tryAcquire with timeout.
                // In production, we NEVER use a raw `throttle.acquire()` which blocks infinitely. 
                // If the API goes down, infinite blocking will exhaust the Tomcat/Netty thread pool.
                // We wait for max 2 seconds. If no permit is available, we fail-fast.
                if (throttle.tryAcquire(2, TimeUnit.SECONDS)) {
                    try {
                        // Critical Section: Only 3 threads can be inside this block simultaneously.
                        int currentActive = activeRequests.incrementAndGet();
                        System.out.println(">>> [" + Thread.currentThread().getName() + "] PERMIT ACQUIRED. Executing " + transactionId + ". (Active Requests: " + currentActive + ")");

                        // Simulate heavy network I/O
                        simulateNetworkCall();

                    } finally {
                        // CRITICAL: Must decrement metrics and release permit in a finally block.
                        // If simulateNetworkCall() throws an Exception, the permit is still returned.
                        activeRequests.decrementAndGet();
                        throttle.release();
                        System.out.println("<<< [" + Thread.currentThread().getName() + "] PERMIT RELEASED for " + transactionId);
                    }
                } else {
                    // Fallback / Fail-Fast logic
                    System.err.println("--- [" + Thread.currentThread().getName() + "] TIMEOUT: Gateway overloaded. Rejecting " + transactionId + " (HTTP 429 Too Many Requests).");
                }
            } catch (InterruptedException e) {
                // Handle thread interruption gracefully
                Thread.currentThread().interrupt();
                System.err.println("[" + Thread.currentThread().getName() + "] Was interrupted while waiting for a permit.");
            }
        }

        private void simulateNetworkCall() {
            try {
                // Simulate a call taking between 1 to 3 seconds
                Thread.sleep(ThreadLocalRandom.current().nextInt(1000, 3000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * ============================================================================
     * 2. IMPLEMENTATION & DEMONSTRATION
     * ============================================================================
     */
    public static void main(String[] args) {
        PaymentApiGateway gateway = new PaymentApiGateway();

        // Simulate a burst of 10 concurrent requests hitting our API
        int totalIncomingRequests = 10;
        ExecutorService threadPool = Executors.newFixedThreadPool(totalIncomingRequests);

        System.out.println("Initiating " + totalIncomingRequests + " concurrent payment requests. API capacity is only 3.\n");

        for (int i = 1; i <= totalIncomingRequests; i++) {
            final String txnId = "TXN-" + 1000 + i;
            threadPool.submit(() -> gateway.processPayment(txnId));
        }

        threadPool.shutdown();

        // ============================================================================
        // 3. IN-CODE DEEP DIVE
        // ============================================================================
        /*
         * WHY SEMAPHORE INSTEAD OF REENTRANT-LOCK OR SYNCHRONIZED?
         * Mutual exclusion (Mutex) primitives like `ReentrantLock` only allow ONE thread
         * at a time. If the backend API can handle 3 concurrent connections, using a Mutex
         * creates a massive artificial bottleneck, reducing throughput by 66%. A Semaphore
         * tracks a "count" of available permits, making it the perfect tool for bounded
         * pooling and throttling.
         *
         * HAPPENS-BEFORE RELATIONSHIPS:
         * The Java Memory Model guarantees that a successful call to `semaphore.release()`
         * Happens-Before any subsequent successful call to `semaphore.acquire()`. If Thread A
         * updates a shared cache and releases a permit, Thread B (which grabs that exact permit)
         * is guaranteed to see Thread A's updates.
         *
         * EDGE CASES / PITFALLS:
         * 1. Lack of Ownership: Unlike a ReentrantLock, a Semaphore does not track WHICH thread
         *    holds a permit. A malicious or buggy thread could call `semaphore.release()` without
         *    ever calling `acquire()`. This would artificially increase the permit count to 4, 5,
         *    or more, completely breaking the rate limiter.
         * 2. Unfairness Starvation: By default, Semaphores are unfair. A new thread can steal a
         *    just-released permit while 10 other threads sit in the queue. In rate limiters,
         *    this causes SLA breaches (some requests timeout while newer ones succeed). We pass
         *    `true` to the constructor to enforce strict FIFO fairness.
         */
    }
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * 1. [Main] Initializes the Semaphore with 3 permits (Fair mode) and spawns 10 threads.
 * 2. [Threads 1, 2, 3] Hit `tryAcquire(2s)`. Because 3 permits exist, they instantly
 *    succeed. Permit count drops to 0. They enter the critical section and sleep (simulate I/O).
 * 3. [Threads 4 through 10] Hit `tryAcquire(2s)`. Permit count is 0. Because we used
 *    a fair Semaphore, the JVM enqueues them in a strict FIFO AQS wait queue. They
 *    begin their 2-second timeout countdown.
 * 4. [Thread 2] Finishes early (e.g., after 1.2s). Calls `release()`. Permit count goes to 1.
 * 5. [Thread 4] Is at the front of the queue. The AQS wakes it up. It acquires the permit
 *    and begins execution.
 * 6. [Threads 5 through 10] After 2.0 seconds have elapsed, no more permits have opened up.
 *    Their `tryAcquire` timers expire.
 * 7. [Threads 5 through 10] The JVM unparks them, `tryAcquire` returns `false`, and they
 *    execute the fail-fast `else` block, rejecting the HTTP requests cleanly instead of
 *    hanging the server.
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: `tryAcquire` and `release` are O(1) atomic Topics.CAS (Compare-And-Swap)
 *   operations under the hood.
 * - Space Complexity: O(1) heap allocation. The AQS Node queue scales proportionally
 *   to the number of waiting threads.
 * - Performance Overhead: Using `new Semaphore(..., true)` (Fairness) introduces a
 *   performance penalty because the OS must context-switch to wake up the *specific*
 *   thread at the head of the queue, rather than allowing any running thread to aggressively
 *   grab the permit. However, in an API Gateway, fairness is usually more critical than
 *   raw nanosecond throughput.
 */