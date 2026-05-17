package Topics.LocksAndConditions.ReentrantLock;

/**
 * ============================================================================
 * HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Demonstrate Externalized Locking (Dependency Injection of a Lock).
 * The lock is managed by a higher-level orchestrator and passed into the
 * worker resources.
 *
 * Real-World Use Case:
 * Multi-resource transactions. E.g., Transferring money between Account A
 * and Account B requires locking BOTH accounts simultaneously. The lock must
 * exist outside of the individual Account objects.
 */

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class ReentrantExternalLockDemonstration {

    /**
     * Shared Resource.
     * Notice it NO LONGER instantiates its own lock. It relies on the caller.
     */
    static class OrderMatchingEngine {

        private int processedOrders = 0;

        // The lock is now passed as a parameter.
        public void processOrder(String orderId, ReentrantLock externalLock) {
            System.out.println("[" + Thread.currentThread().getName() + "] Attempting to acquire provided lock...");

            try {
                // We use the lock provided by the caller
                if (externalLock.tryLock(2, TimeUnit.SECONDS)) {
                    try {
                        System.out.println("[" + Thread.currentThread().getName() + "] Lock ACQUIRED. Processing " + orderId);
                        Thread.sleep(1000);
                        processedOrders++;
                    } finally {
                        externalLock.unlock();
                        System.out.println("[" + Thread.currentThread().getName() + "] Lock RELEASED.");
                    }
                } else {
                    System.err.println("[" + Thread.currentThread().getName() + "] TIMEOUT: Lock is held elsewhere.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * ============================================================================
     * IMPLEMENTATION (The Orchestrator)
     * ============================================================================
     */
    public static void main(String[] args) {
        OrderMatchingEngine engine = new OrderMatchingEngine();

        // 1. The lock is created outside the resource.
        // It is now the responsibility of the Orchestrator to manage this lifecycle.
        ReentrantLock sharedOrchestratorLock = new ReentrantLock(true);

        Runnable task = () -> {
            String orderId = "ORD-" + (int)(Math.random() * 1000);
            // 2. We inject the lock into the resource method.
            engine.processOrder(orderId, sharedOrchestratorLock);
        };

        Thread t1 = new Thread(task, "Thread-1");
        Thread t2 = new Thread(task, "Thread-2");

        t1.start();
        t2.start();

        // ARCHITECTURAL DANGER DEMONSTRATION:
        // Because the lock is external, 'main' could maliciously or accidentally
        // acquire it right now and never release it, starving T1 and T2 completely.
        // sharedOrchestratorLock.lock();
    }
}