package Topics.LocksTypeMonitorLocks.SameObjectLock;

/**
 * ======================================================================================
 * INTER-THREAD COMMUNICATION: PRODUCER-CONSUMER PROBLEM
 * ======================================================================================
 *
 * PROBLEM STATEMENT:
 * In multithreading, sometimes a thread (Consumer) relies on the output of another
 * thread (Producer). If the Producer hasn't finished its job yet, the Consumer needs
 * to wait. However, we don't want the Consumer to repeatedly check the status in a
 * "busy loop" (e.g., `while(!available) {}`) because that wastes massive CPU resources.
 *
 * THE SOLUTION:
 * Java provides `wait()` and `notify()` / `notifyAll()` methods specifically for this.
 * - `wait()`: Tells the current thread to release the monitor lock and go to sleep.
 * - `notifyAll()`: Wakes up all threads that are waiting on this exact monitor lock.
 *
 * SOLUTION STEPS / EXECUTION FLOW:
 * 1. Consumer thread enters `consumerItem()` and acquires the lock.
 * 2. Consumer checks if `itemAvailable` is true. It is false.
 * 3. Consumer calls `wait()`. This immediately PAUSES the consumer and RELEASES the lock.
 * 4. Because the lock is free, the Producer thread enters `addItem()` and acquires the lock.
 * 5. Producer creates the item, sets `itemAvailable = true`, and calls `notifyAll()`.
 * 6. Producer finishes and releases the lock.
 * 7. Consumer wakes up, re-acquires the lock, checks the `while` loop condition again,
 *    sees `itemAvailable` is now true, breaks the loop, and consumes the item!
 * ======================================================================================
 */
public class SharedResourceExample3 {
    boolean itemAvailable = false;

    // synchronized means a thread MUST acquire the monitor lock of this object to enter
    public synchronized void addItem() {
        System.out.println("[PRODUCER] Acquired lock. Generating item...");
        itemAvailable = true;
        System.out.println("[PRODUCER] Item state changed to AVAILABLE.");

        System.out.println("[PRODUCER] Invoking notifyAll() to wake up waiting threads...");
        // notifyAll() wakes up any threads currently stuck in the wait() state for this object.
        // NOTE: It does not instantly give them the lock; they must wait for this method to finish.
        notifyAll();

        System.out.println("[PRODUCER] Finished addItem() logic, releasing lock.");
    }

    // synchronized means the consumer MUST acquire the same lock as the producer
    public synchronized void consumerItem() {
        System.out.println("[CONSUMER] Acquired lock. Checking if item is available...");

        // CRITICAL: We use a 'while' loop instead of an 'if' statement to avoid "Spurious Wakeups".
        // A spurious wakeup is an OS-level glitch where a waiting thread randomly wakes up
        // without notify() being called. The while loop forces it to re-check the condition!
        while (!itemAvailable) {
            try {
                System.out.println("[CONSUMER] Item NOT available. Releasing lock and going to WAIT state...");
                // wait() pauses this thread and totally gives up the lock so the Producer can use it.
                wait();
                System.out.println("[CONSUMER] Woke up from wait state! Re-acquiring lock and re-checking condition...");
            } catch (InterruptedException e) {
                // Good practice to print the stack trace in catch blocks rather than just getting the message
                e.printStackTrace();
            }
        }

        System.out.println("[CONSUMER] Item IS available! Consuming item now...");
        itemAvailable = false; // Reset the flag after consuming
        System.out.println("[CONSUMER] Item consumed. Finished consumerItem() logic, releasing lock.");
    }
}

class ProducerTask implements Runnable {
    SharedResourceExample3 sharedResource;

    ProducerTask(SharedResourceExample3 sharedResource) {
        this.sharedResource = sharedResource;
    }

    @Override
    public void run() {
        System.out.println("[THREAD-TRACE] Producer Thread started: " + Thread.currentThread().getName());
        try {
            // Simulate heavy processing time (5 seconds) to produce the item
            System.out.println("[PRODUCER] Taking 5 seconds to build the item...");
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Once built, try to add the item (requires acquiring the lock)
        sharedResource.addItem();
        System.out.println("[THREAD-TRACE] Producer Thread " + Thread.currentThread().getName() + " done and exited.");
    }
}

class ConsumerTask implements Runnable {
    SharedResourceExample3 sharedResource;

    ConsumerTask(SharedResourceExample3 sharedResource) {
        this.sharedResource = sharedResource;
    }

    @Override
    public void run() {
        System.out.println("[THREAD-TRACE] Consumer Thread started: " + Thread.currentThread().getName());
        try {
            // Reduced sleep time to 1 second so the Consumer definitely arrives at the shared resource first.
            // This guarantees we see the wait() -> notify() logic in action in the logs.
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Try to consume the item (requires acquiring the lock)
        sharedResource.consumerItem();
        System.out.println("[THREAD-TRACE] Consumer Thread " + Thread.currentThread().getName() + " done and exited.");
    }
}

class SharedResourceExample3Main {
    public static void main(String[] args) {
        System.out.println("========== MAIN METHOD START WITH MAIN THREAD ==========");

        // Only ONE shared object instance. Both threads will fight for this object's monitor lock.
        SharedResourceExample3 sharedResource = new SharedResourceExample3();

        // Pass the shared resource to both tasks
        Thread producerThread = new Thread(new ProducerTask(sharedResource), "Thread-Producer");
        Thread consumerThread = new Thread(new ConsumerTask(sharedResource), "Thread-Consumer");


//        Lambda code
//        Thread consumerThread1 = new Thread(
//                () -> {
//                    System.out.println("Consumer thread: " + Thread.currentThread().getName());
//                    sharedResource.consumerItem();
//                }, "Thread-Consumer"
//        );

        // Start both threads (Moves them to RUNNABLE state)
        producerThread.start();
        consumerThread.start();

        System.out.println("========== MAIN METHOD END WITH MAIN THREAD ==========");
        // Note: The main thread finishes instantly, but the JVM keeps running
        // until the producer and consumer threads finish their work.
    }
}