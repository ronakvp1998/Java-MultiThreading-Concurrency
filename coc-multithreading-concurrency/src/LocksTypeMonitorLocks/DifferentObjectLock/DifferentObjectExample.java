package LocksTypeMonitorLocks.DifferentObjectLock;

/**
 * SCENARIO 2: INSTANCE LOCK WITH DIFFERENT OBJECTS
 *
 * PROBLEM STATEMENT:
 * If threads are working on completely independent data, forcing them to wait
 * in line creates a massive performance bottleneck.
 *
 * USE CASE:
 * An E-commerce website. User A is adding an item to their Shopping Cart, and
 * User B is adding an item to their completely separate Shopping Cart.
 *
 * GOAL:
 * Maximize performance. Allow threads to run simultaneously (Parallel Execution)
 * because they are not modifying the same data.
 *
 * HOW TO ACHIEVE IT:
 * 1. Use the `synchronized` keyword on the instance method (for thread-safety per cart).
 * 2. Pass completely DIFFERENT object instances to the threads.
 */

class ShoppingCart {
    private int itemCount = 0;

    // Locks only the specific instance of ShoppingCart calling it
    public synchronized void addItem(String itemName, String threadName) {
        System.out.println(threadName + " started adding " + itemName + " to their cart.");

        try {
            // Simulating a database call to add the item
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        itemCount++;
        System.out.println(threadName + " successfully added " + itemName + ". Total items: " + itemCount);
    }
}

public class DifferentObjectExample {
    public static void main(String[] args) {
        // We create TWO completely separate Shopping Carts
        ShoppingCart cartForUserA = new ShoppingCart();
        ShoppingCart cartForUserB = new ShoppingCart();

        // Threads use different objects, so they get different monitor locks
        Thread userAThread = new Thread(() -> cartForUserA.addItem("Laptop", "User A"));
        Thread userBThread = new Thread(() -> cartForUserB.addItem("Headphones", "User B"));

        userAThread.start();
        userBThread.start();
    }
}
