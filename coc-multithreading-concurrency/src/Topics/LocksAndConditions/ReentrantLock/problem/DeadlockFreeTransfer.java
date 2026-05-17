package Topics.LocksAndConditions.ReentrantLock.problem;

/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Safely transfer funds between two bank accounts. If Thread 1 transfers from
 * Account A to Account B, and Thread 2 concurrently transfers from Account B
 * to Account A, using standard `synchronized` blocks will cause a classic
 * Deadlock (Circular Wait). Design a solution that prevents deadlocks.
 *
 * Real-World Use Case:
 * Core banking ledgers, distributed inventory systems (moving stock between
 * two warehouses), or multi-row database transaction locking.
 *
 * Concurrency Constraints:
 * - Deadlock Prevention: Threads must never block infinitely waiting for a lock.
 * - Livelock Prevention: Threads backing off must not fall into a synchronized
 *   retry loop where they continuously block each other.
 * - Atomicity: Funds must either fully transfer or not transfer at all.
 */

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DeadlockFreeTransfer {

    /**
     * Shared Resource representing a Bank Account.
     */
    static class Account {
        private final String accountId;
        private int balance;
        // The lock is encapsulated within the account
        final Lock lock = new ReentrantLock();

        public Account(String accountId, int initialBalance) {
            this.accountId = accountId;
            this.balance = initialBalance;
        }

        public void withdraw(int amount) { balance -= amount; }
        public void deposit(int amount) { balance += amount; }
        public int getBalance() { return balance; }
        public String getAccountId() { return accountId; }
    }

    /**
     * Service class handling the transaction.
     */
    static class TransferService {

        public void transfer(Account fromAccount, Account toAccount, int amount) {
            System.out.println("[" + Thread.currentThread().getName() + "] Initiating transfer: " + fromAccount.getAccountId() + " -> " + toAccount.getAccountId());

            // Loop until the transfer is successfully completed
            while (true) {
                boolean fromLockAcquired = false;
                boolean toLockAcquired = false;

                try {
                    // SENIOR PATTERN: Backoff and Retry using tryLock()
                    // Attempt to acquire both locks without blocking indefinitely.
                    fromLockAcquired = fromAccount.lock.tryLock();
                    toLockAcquired = toAccount.lock.tryLock();

                    if (fromLockAcquired && toLockAcquired) {
                        // CRITICAL SECTION: We own BOTH locks. It is 100% safe to mutate.
                        if (fromAccount.getBalance() >= amount) {
                            fromAccount.withdraw(amount);
                            toAccount.deposit(amount);
                            System.out.println("[" + Thread.currentThread().getName() + "] SUCCESS: Transferred $" + amount + ". " +
                                    fromAccount.getAccountId() + " balance: " + fromAccount.getBalance() + ", " +
                                    toAccount.getAccountId() + " balance: " + toAccount.getBalance());
                            break; // Exit the while loop
                        } else {
                            System.err.println("[" + Thread.currentThread().getName() + "] FAILED: Insufficient funds in " + fromAccount.getAccountId());
                            break; // Exit the while loop
                        }
                    } else {
                        System.out.println("[" + Thread.currentThread().getName() + "] Contention detected. Retrying...");
                    }

                } finally {
                    // ALWAYS release locks in a finally block to prevent permanent resource lockup.
                    // We only unlock if we successfully acquired it in this iteration.
                    if (fromLockAcquired) {
                        fromAccount.lock.unlock();
                    }
                    if (toLockAcquired) {
                        toAccount.lock.unlock();
                    }
                }

                // LIVELOCK PREVENTION:
                // If T1 and T2 both fail, we don't want them to instantly retry at the
                // exact same time. We add a random "jitter" to offset their retry cycles.
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(10, 50));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("[" + Thread.currentThread().getName() + "] Transfer interrupted.");
                    break;
                }
            }
        }
    }

    /**
     * ============================================================================
     * 2. IMPLEMENTATION & DEMONSTRATION
     * ============================================================================
     */
    public static void main(String[] args) {
        Account accA = new Account("ACC-A", 1000);
        Account accB = new Account("ACC-B", 1000);
        TransferService service = new TransferService();

        // Thread 1 attempts A -> B
        Thread t1 = new Thread(() -> service.transfer(accA, accB, 200), "Tx-Thread-1");

        // Thread 2 simultaneously attempts B -> A (The classic Deadlock scenario)
        Thread t2 = new Thread(() -> service.transfer(accB, accA, 300), "Tx-Thread-2");

        t1.start();
        t2.start();

        // ============================================================================
        // 3. IN-CODE DEEP DIVE
        // ============================================================================
        /*
         * WHY REENTRANTLOCK TRY-LOCK OVER SYNCHRONIZED?
         * If we used `synchronized(fromAccount) { synchronized(toAccount) { ... } }`:
         * T1 locks A. T2 locks B. T1 waits forever for B. T2 waits forever for A. DEADLOCK.
         * By using `tryLock()`, if T1 gets A but fails to get B, it explicitly RELEASES A
         * and tries again. It politely steps back so T2 can finish.
         *
         * HAPPENS-BEFORE RELATIONSHIPS:
         * When T1 unlocks A and B, the JMM forces a flush of `balance` to Main Memory.
         * When T2 subsequently successfully calls `tryLock()` on those accounts, it acts as
         * a memory barrier, pulling the freshest `balance` values from Main Memory.
         *
         * EDGE CASES / PITFALLS:
         * Livelock: If T1 and T2 continuously grab their first lock, fail to get the second,
         * release, and retry at the exact same millisecond, they will loop forever without
         * making progress. We defeat this using `ThreadLocalRandom.current().nextInt(10, 50)`
         * to desynchronize their retry attempts.
         */
    }
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * 1. T1 attempts to lock A. T2 attempts to lock B. Both succeed.
 * 2. T1 attempts `tryLock()` on B. It returns false (because T2 holds it).
 * 3. T2 attempts `tryLock()` on A. It returns false (because T1 holds it).
 * 4. T1 enters `finally` block and releases A.
 * 5. T2 enters `finally` block and releases B.
 * 6. T1 sleeps for a random time (e.g., 12ms). T2 sleeps for a random time (e.g., 40ms).
 * 7. T1 wakes up first. It successfully calls `tryLock()` on A and then B.
 * 8. T1 executes the transfer, updates balances, and releases A and B.
 * 9. T2 wakes up, acquires B and A, executes its transfer, and releases both.
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time Complexity: O(1) in the best case. In high-contention scenarios, time
 *   complexity degrades due to the spin-retry backoff algorithm.
 * - Space Complexity: O(1).
 * - Overhead: `tryLock` is an efficient atomic Topics.CAS (Compare-And-Swap) operation under
 *   the hood via Java's AbstractQueuedSynchronizer (AQS). The primary overhead here
 *   is the OS context switching caused by `Thread.sleep()`. In extreme low-latency (HFT),
 *   we would use `Thread.yield()` or a `Thread.onSpinWait()` instead of `sleep()`.
 */