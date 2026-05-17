package Topics.LocksTypeMonitorLocks.SameObjectLock;

/**
 * SCENARIO 1: INSTANCE LOCK WITH THE SAME OBJECT
 *
 * PROBLEM STATEMENT:
 * If multiple threads try to read and modify the exact same piece of data at
 * the exact same time, they can overwrite each other's work (a Race Condition).
 *
 * USE CASE:
 * A Bank Account. An ATM and an Auto-Bill Pay system are both trying to
 * withdraw money from the exact same account at the same time.
 *
 * GOAL:
 * Protect the shared data. Force the threads to form a line and execute one
 * at a time (Sequential Execution) when accessing this specific object.
 *
 * HOW TO ACHIEVE IT:
 * 1. Use the `synchronized` keyword on the instance method.
 * 2. Pass the exact SAME object instance to both threads.
 */

class BankAccount {
    private int balance = 100;

    // The 'synchronized' keyword locks the specific instance of BankAccount
    public synchronized void withdraw(int amount, String threadName) {
        System.out.println(threadName + " is accessing the account. Lock acquired.");

        if (balance >= amount) {
            System.out.println(threadName + " sees enough balance. Withdrawing $" + amount);
            try {
                // Simulating the time it takes to process the transaction
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            balance -= amount;
            System.out.println(threadName + " finished. New balance: $" + balance);
        } else {
            System.out.println(threadName + " failed. Insufficient funds.");
        }

        System.out.println(threadName + " is releasing the lock.\n");
    }
}

public class SameObjectExample {
    public static void main(String[] args) {
        // We create ONLY ONE Bank Account
        BankAccount sharedAccount = new BankAccount();

        // Both threads share the exact same object
        Thread atmThread = new Thread(() -> sharedAccount.withdraw(50, "ATM"));
        Thread autoPayThread = new Thread(() -> sharedAccount.withdraw(50, "AutoPay"));

        atmThread.start();
        autoPayThread.start();
    }
}