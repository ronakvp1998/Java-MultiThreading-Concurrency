package LocksTypeMonitorLocks.ClassLevelLocks;

/**
 * SCENARIO 3: CLASS-LEVEL LOCK (STATIC SYNCHRONIZED)
 *
 * PROBLEM STATEMENT:
 * Sometimes data doesn't belong to an object instance; it belongs to the
 * Class itself (a static variable). If multiple threads from different instances
 * try to modify a static variable, they will corrupt it.
 *
 * USE CASE:
 * A Global Invoice Number Generator. Every time an order is processed, the system
 * needs to generate a unique, sequential invoice number across the whole app.
 *
 * GOAL:
 * Protect global/static data. Ensure that only one thread in the ENTIRE Java Virtual
 * Machine can execute this method at a time, regardless of what object instance it uses.
 *
 * HOW TO ACHIEVE IT:
 * 1. Use the `static` and `synchronized` keywords together.
 * 2. The lock now belongs to the Class blueprint (InvoiceGenerator.class), not the object.
 */

class InvoiceGenerator {
    // A global variable shared across the whole application
    private static int latestInvoiceNumber = 1000;

    // The 'static' keyword moves the lock from the instance to the Class itself
    public static synchronized void generateNextInvoice(String threadName) {
        System.out.println(threadName + " acquired the CLASS lock and is generating an invoice.");

        try {
            // Simulating system processing time
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        latestInvoiceNumber++;
        System.out.println(threadName + " generated Invoice #" + latestInvoiceNumber);
        System.out.println(threadName + " released the CLASS lock.\n");
    }
}

public class ClassLockExample {
    public static void main(String[] args) {
        // Creating two different instances of worker objects
        InvoiceGenerator worker1 = new InvoiceGenerator();
        InvoiceGenerator worker2 = new InvoiceGenerator();

        // Even though they use DIFFERENT objects (worker1 and worker2),
        // calling a static synchronized method forces them to wait in line
        // for the single global Class lock.
        Thread t1 = new Thread(() -> worker1.generateNextInvoice("Worker-1"));
        Thread t2 = new Thread(() -> worker2.generateNextInvoice("Worker-2"));

        t1.start();
        t2.start();
    }
}