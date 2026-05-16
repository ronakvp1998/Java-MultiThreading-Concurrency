package ThreadJoiningDeamonPriority.ProducerConsumerProblem;

import java.util.LinkedList;
import java.util.Queue;

/**
 * ======================================================================================
 * THE PRODUCER-CONSUMER PROBLEM (BOUNDED BUFFER)
 * ======================================================================================
 *
 * PROBLEM STATEMENT:
 * Two threads (Producer and Consumer) share a common, fixed-size memory buffer (Queue).
 * - If the Producer works faster than the Consumer, the buffer gets full. If the Producer
 *   keeps adding, it will crash or overwrite data (Overflow).
 * - If the Consumer works faster than the Producer, the buffer gets empty. If the Consumer
 *   keeps trying to read, it will crash or read null data (Underflow).
 *
 * THE GOAL:
 * The threads must communicate.
 * - The Producer must PAUSE if the buffer is full and wait for the Consumer to remove an item.
 * - The Consumer must PAUSE if the buffer is empty and wait for the Producer to add an item.
 *
 * SOLUTION STEPS:
 * 1. Mutual Exclusion: Use `synchronized` methods so only one thread accesses the Queue at a time.
 * 2. Condition Checking: Use a `while` loop to check if the buffer is full/empty.
 * 3. Pausing (`wait()`): If the condition is met, release the lock and go to sleep.
 * 4. Waking Up (`notify()`): Once a thread successfully adds/removes an item, it signals
 *    the other sleeping thread to wake up and check the condition again.
 * ======================================================================================
 */
public class SharedResource {

    private Queue<Integer> sharedBuffer;
    private int bufferSize;

    public SharedResource(int bufferSize){
        sharedBuffer = new LinkedList<>();
        this.bufferSize = bufferSize;
    }

    /**
     * PRODUCER LOGIC
     * 'synchronized' forces the producer thread to acquire the lock for this SharedResource object.
     */
    public synchronized void producer(int item) throws Exception {
        System.out.println("[PRODUCER] Acquired lock. Attempting to produce item: " + item);

        // CRITICAL: We use 'while' instead of 'if' to prevent "Spurious Wakeups" (OS-level glitches
        // where threads wake up randomly without being notified). The thread MUST re-check the condition.
        while (sharedBuffer.size() == bufferSize) {
            System.out.println("[PRODUCER] 🛑 Buffer is FULL (Size: " + sharedBuffer.size() + "). Producer is WAITING for Consumer.");
            // wait() immediately pauses the Producer AND releases the lock so the Consumer can jump in.
            wait();
            System.out.println("[PRODUCER] 🟢 Woke up! Re-checking buffer size...");
        }

        // Add the item to the buffer
        sharedBuffer.add(item);
        System.out.println("[PRODUCER] ✅ Successfully Produced: " + item + " | Buffer size now: " + sharedBuffer.size());

        // Notify the waiting Consumer that the buffer is no longer empty.
        // (notify() wakes up a single thread, notifyAll() wakes up all threads. Since we only have 2 threads, notify() is fine).
        System.out.println("[PRODUCER] Invoking notify() to wake up the Consumer.\n");
        notify();
    }

    /**
     * CONSUMER LOGIC
     * 'synchronized' forces the consumer thread to acquire the lock for this SharedResource object.
     */
    public synchronized int consumer() throws Exception {
        System.out.println("[CONSUMER] Acquired lock. Attempting to consume an item...");

        // CRITICAL: Wait in a 'while' loop if the buffer is empty
        while (sharedBuffer.isEmpty()) {
            System.out.println("[CONSUMER] 🛑 Buffer is EMPTY. Consumer is WAITING for Producer.");
            // wait() pauses the Consumer and gives the lock back to the Producer so it can make an item.
            wait();
            System.out.println("[CONSUMER] 🟢 Woke up! Re-checking if buffer has items...");
        }

        // poll() removes and returns the first element of the Queue
        int item = sharedBuffer.poll();
        System.out.println("[CONSUMER] ✅ Successfully Consumed: " + item + " | Buffer size now: " + sharedBuffer.size());

        // Notify the waiting Producer that space has just cleared up in the buffer.
        System.out.println("[CONSUMER] Invoking notify() to wake up the Producer.\n");
        notify();

        return item;
    }
}

class ProducerConsumerProblemSolutionMain {
    public static void main(String[] args) {
        System.out.println("========== SYSTEM START ==========\n");

        // Create a shared resource with a maximum buffer capacity of 3
        SharedResource sharedResource = new SharedResource(3);

        // ---------------------------------------------------------
        // 1. CREATE PRODUCER THREAD
        // ---------------------------------------------------------
        Thread producerThread = new Thread(
                () -> {
                    try {
                        for (int i = 1; i <= 6; i++) {
                            sharedResource.producer(i);

                            // Adding a slight delay to simulate processing time
                            // and make the console output easier to read
                            Thread.sleep(500);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, "Thread-Producer" // Naming the thread for clarity
        );

        // ---------------------------------------------------------
        // 2. CREATE CONSUMER THREAD
        // ---------------------------------------------------------
        Thread consumerThread = new Thread(
                () -> {
                    try {
                        for (int i = 1; i <= 6; i++) {
                            sharedResource.consumer();

                            // Making the consumer slightly slower than the producer
                            // to force the buffer to fill up and trigger the Producer's wait() state
                            Thread.sleep(800);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, "Thread-Consumer"
        );

        // Start both threads
        producerThread.start();
        consumerThread.start();

        System.out.println("========== MAIN THREAD FINISHED (Waiting for workers) ==========\n");
    }
}