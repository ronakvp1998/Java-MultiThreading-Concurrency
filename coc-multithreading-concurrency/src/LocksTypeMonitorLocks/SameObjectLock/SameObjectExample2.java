package LocksTypeMonitorLocks.SameObjectLock;

/**
 * SCENARIO: MIXING SYNCHRONIZED METHODS, SYNCHRONIZED BLOCKS, AND NON-SYNCHRONIZED METHODS
 *
 * PROBLEM STATEMENT:
 * Sometimes a method does a lot of work, but only a tiny portion of that work
 * actually modifies shared data. If you make the entire method `synchronized`,
 * threads will be blocked for longer than necessary, hurting application performance.
 *
 * USE CASE:
 * Imagine a method that downloads a file (takes 5 seconds) and then updates a shared
 * counter (takes 1 millisecond). You don't want threads waiting 5 seconds just to
 * update a counter.
 *
 * GOAL:
 * Demonstrate how to optimize locks by using a `synchronized(this)` block to only
 * lock the critical section, and show how synchronized blocks interact with fully
 * synchronized methods and completely unsynchronized methods on the SAME object.
 *
 * HOW IT WORKS IN THIS CODE:
 * - thread1 grabs the lock for `obj1` to run task1().
 * - thread2 can start task2(), but will freeze as soon as it hits the synchronized block.
 * - thread3 runs task3() completely freely because it requires no lock at all.
 */
public class SameObjectExample2 {

    /**
     * TASK 1: FULL METHOD SYNCHRONIZATION
     * Because the method signature has 'synchronized', the thread must acquire
     * the intrinsic lock of the object instance ('this') before executing line 1.
     */
    public synchronized void task1(){
        try{
            System.out.println("inside Task1 method - Lock Acquired!");
            // Simulating a long-running process while holding the lock
            Thread.sleep(1000);
            System.out.println("task1 done - Lock Released!");
        } catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    /**
     * TASK 2: BLOCK SYNCHRONIZATION (Optimized Locking)
     * This method does NOT require a lock to start. Any thread can enter this method.
     * It only requires the lock when it reaches the `synchronized(this)` block.
     */
    public void task2(){
        // Thread 2 will print this immediately, even if Thread 1 is holding the lock in task1!
        System.out.println("inside Task2 method - No lock needed here.");

        // This is the Critical Section. Thread 2 needs the lock for 'this' object.
        // If Thread 1 is still inside task1(), Thread 2 will PAUSE right here and wait.
        synchronized (this){
            System.out.println("inside Task2 synchronized block - Lock Acquired!");
        }

        // Executes after the block finishes and lock is released
        System.out.println("task2 done");
    }

    /**
     * TASK 3: NO SYNCHRONIZATION
     * This method has no synchronized keyword and no synchronized blocks.
     * It completely ignores locks. Thread 3 will execute this instantly,
     * regardless of what Thread 1 or Thread 2 are doing.
     */
    public void task3(){
        System.out.println("inside task3 - Completely independent, no lock needed!");
    }

    public static void main(String[] args) {
        // We create exactly ONE object instance.
        // This means there is only ONE lock to fight over.
        SameObjectExample2 obj1 = new SameObjectExample2();

        // Pass the same object to all three threads
        Thread thread1 = new Thread(() -> obj1.task1());
        Thread thread2 = new Thread(() -> obj1.task2());
        Thread thread3 = new Thread(() -> obj1.task3());

        // Start all threads at roughly the exact same time
        thread1.start();
        thread2.start();
        thread3.start();
    }
}