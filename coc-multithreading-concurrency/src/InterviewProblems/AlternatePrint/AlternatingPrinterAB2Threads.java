package InterviewProblems.AlternatePrint;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AlternatingPrinterAB2Threads {

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Printer sharedPrinter = new Printer();

        executor.submit(() -> sharedPrinter.print("A", true));
        executor.submit(() -> sharedPrinter.print("B", false));

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

class Printer {
    private boolean isTurnA = true;
    private final int MAX_PRINT_COUNT = 10;
    private int currentCount = 0;

    // 1. Declare the Lock and Condition
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    public void print(String text, boolean isThreadA) {
        while (currentCount < MAX_PRINT_COUNT) {
            // 2. Acquire the lock before entering the critical section
            lock.lock();
            try {
                while (isTurnA != isThreadA && currentCount < MAX_PRINT_COUNT) {
                    // 3. await() replaces wait()
                    condition.await();
                }

                if (currentCount >= MAX_PRINT_COUNT) {
                    // Wake up the other thread before breaking so it doesn't wait forever
                    condition.signalAll();
                    break;
                }

                System.out.print(text + " ");
                currentCount++;

                isTurnA = !isTurnA;

                // 4. signalAll() replaces notifyAll()
                condition.signalAll();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } finally {
                // 5. Always unlock in a finally block to prevent deadlocks if an exception occurs
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
//public class AlternatingPrinterAB2Threads {
//
//    public static void main(String[] args) {
//        // Create an ExecutorService with exactly 2 threads
//        ExecutorService executor = Executors.newFixedThreadPool(2);
//
//        Printer sharedPrinter = new Printer();
//
//        // Submit Thread 1 to print 'A'
//        executor.submit(() -> sharedPrinter.print("A", true));
//
//        // Submit Thread 2 to print 'B'
//        executor.submit(() -> sharedPrinter.print("B", false));
//
//        // Initiate an orderly shutdown
//        executor.shutdown();
//        try {
//            executor.awaitTermination(1, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//        }
//    }
//}
//
//class Printer {
//    // Flag to track whose turn it is. True = A's turn, False = B's turn.
//    private boolean isTurnA = true;
//    private final int MAX_PRINT_COUNT = 10;
//    private int currentCount = 0;
//
//    public synchronized void print(String text, boolean isThreadA) {
//        while (currentCount < MAX_PRINT_COUNT) {
//            // Wait if it's not this thread's turn
//            while (isTurnA != isThreadA && currentCount < MAX_PRINT_COUNT) {
//                try {
//                    wait();
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt();
//                    return;
//                }
//            }
//
//            // Double-check count after waking up
//            if (currentCount >= MAX_PRINT_COUNT) {
//                break;
//            }
//
//            System.out.print(text + " ");
//            currentCount++;
//
//            // Toggle the turn and wake up the other thread
//            isTurnA = !isTurnA;
//            notifyAll();
//        }
//    }
//}