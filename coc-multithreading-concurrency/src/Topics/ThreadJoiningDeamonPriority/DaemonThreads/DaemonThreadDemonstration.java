package Topics.ThreadJoiningDeamonPriority.DaemonThreads;
/**
 * ============================================================================
 * 1. HEADER DOCUMENTATION
 * ============================================================================
 * Problem Statement:
 * Demonstrate the lifecycle and behavioral differences between User Threads
 * and Daemon Threads. Specifically, illustrate how the JVM abruptly halts
 * Daemon threads when all User threads complete their execution.
 *
 * Real-World Use Case:
 * - The JVM Garbage Collector (GC).
 * - Background telemetry/metrics reporters (e.g., DataDog agents).
 * - Connection keep-alive (heartbeat) monitors in distributed systems.
 *
 * Concurrency Constraints:
 * - Abrupt Termination: The JVM does NOT wait for Daemon threads to finish.
 *   It kills them instantly when the last User thread dies.
 * - Resource Leaks: Because termination is abrupt, `finally` blocks in Daemon
 *   threads are NOT guaranteed to execute. Therefore, Daemon threads must NEVER
 *   be used for I/O operations (file writes, database transactions) as they
 *   will cause data corruption or leave open file handles.
 */

public class DaemonThreadDemonstration {

    /**
     * A background service that runs continuously.
     * In a real system, this might poll for memory usage or send heartbeats.
     */
    static class BackgroundMonitor implements Runnable {
        @Override
        public void run() {
            try {
                // Infinite loop: standard pattern for background daemon tasks
                while (true) {
                    System.out.println("[" + Thread.currentThread().getName() + "] Heartbeat: System is healthy...");
                    Thread.sleep(500);
                }
            } catch (InterruptedException e) {
                // Good practice, though rarely hit in a Daemon thread during JVM shutdown
                Thread.currentThread().interrupt();
                System.out.println("[" + Thread.currentThread().getName() + "] Interrupted!");
            } finally {
                // CRITICAL INTERVIEW POINT:
                // This finally block is NOT guaranteed to execute when the JVM halts!
                System.out.println("⚠️ [" + Thread.currentThread().getName() + "] Cleaning up resources (You might never see this!)...");
            }
        }
    }

    /**
     * ============================================================================
     * 2. IMPLEMENTATION & DEMONSTRATION
     * ============================================================================
     */
    public static void main(String[] args) {
        System.out.println("[" + Thread.currentThread().getName() + "] JVM Started.");

        Thread daemonThread = new Thread(new BackgroundMonitor(), "Daemon-Monitor");

        // setDaemon(true) MUST be called BEFORE start().
        // Throwing IllegalThreadStateException otherwise.
        daemonThread.setDaemon(true);
        daemonThread.start();

        // Simulate the Main thread (User Thread) doing core business logic
        try {
            System.out.println("[" + Thread.currentThread().getName() + "] Processing core application logic...");
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // ============================================================================
        // 3. IN-CODE DEEP DIVE
        // ============================================================================
        /*
         * WHY THIS APPROACH?
         * Your original snippet attempted to use a Daemon thread for a crucial
         * initialization task (`produce()`). That is an anti-pattern. If the Main
         * thread doesn't wait (via `.join()`), the JVM exits before `isAvailable = true`
         * ever executes. Daemon threads are strictly for non-critical, expendable
         * background tasks.
         *
         * HAPPENS-BEFORE RELATIONSHIP:
         * Calling `daemonThread.start()` Happens-Before any action in the new thread.
         * However, because the JVM does not call `.join()` on daemon threads during
         * shutdown, there is NO Happens-Before relationship established at the end
         * of their lifecycle. Any state mutated by a dying Daemon thread is highly
         * unsafe to read.
         *
         * THE MODERN "SENIOR" ALTERNATIVE:
         * While `setDaemon(true)` is conceptually important, modern Java applications
         * rarely manage raw daemon threads. We use a `ScheduledExecutorService` with a
         * custom ThreadFactory that creates daemon threads, allowing for graceful
         * shutdown hooks via `executor.shutdown()`.
         */

        System.out.println("[" + Thread.currentThread().getName() + "] Application logic complete. Main thread exiting.");
        System.out.println("Notice how the Daemon thread is instantly killed by the OS. The JVM shuts down now.");
    }
}

/*
 * ============================================================================
 * 4. STEP-BY-STEP EXECUTION LOGIC
 * ============================================================================
 * 1. [JVM] Starts and spawns the `main` User Thread.
 * 2. [Main Thread] Creates `daemonThread`, sets its flag to Daemon, and starts it.
 * 3. [Main Thread] Sleeps for 2000ms.
 * 4. [Daemon Thread] Wakes up every 500ms and prints its heartbeat. It does this ~4 times.
 * 5. [Main Thread] Wakes up, prints its exit message, and terminates.
 * 6. [JVM] Scans the Thread Registry. It sees that ZERO User Threads are alive.
 * 7. [JVM] Initiates immediate halt sequence. It drops the `Daemon-Monitor` thread
 *    without waiting for it to finish its current instruction or execute its `finally` block.
 * 8. [OS] Reclaims the process memory.
 *
 * ============================================================================
 * 5. COMPLEXITY & PERFORMANCE ANALYSIS
 * ============================================================================
 * - Time/Space Complexity: O(1) algorithmic overhead. Standard ~1MB native stack
 *   allocation per thread.
 * - Performance Overhead: Low. Daemon threads are scheduled by the OS exactly like
 *   User threads. They receive the same CPU time slices based on thread priority.
 *   The ONLY difference is how the JVM's shutdown hook observes them.
 */


/*
To understand Daemon threads, it helps to use a simple analogy: Think of a restaurant.

User Threads (Normal Threads) are the paying customers.
The restaurant must stay open as long as even one customer is still eating.

Daemon Threads are the background music and the air conditioning.
They provide supporting services. But the moment the last customer (User Thread) leaves, the manager flips the master switch.
The lights, the music, and the AC are instantly cut off.
The manager does not wait for the current song to finish playing.

Here is the technical breakdown of why we need them, how they differ, and where they are used.

The Core Difference: JVM Shutdown Behavior

The fundamental difference between a Normal (User) Thread and a Daemon Thread
lies in how the JVM treats them during shutdown.

User Threads: The JVM will stay alive until all active User Threads have completed their execution.
Daemon Threads: The JVM completely ignores Daemon threads when deciding to shut down.
If the only threads left running in your application are Daemon threads, the JVM will abruptly halt them and exit the program.

Why Do We Need Them?

If Java only had normal User Threads, writing background tasks would be a nightmare.
Imagine you write a "System Health Monitor" thread that loops infinitely (while(true)),
checking memory usage every 5 seconds. If this was a normal thread, your Java application would never be able to close.
Even if your main program finished its job, that infinite loop would keep the JVM alive forever.

By making the health monitor a Daemon thread, it can run infinitely in the background,
but it won't prevent the application from naturally shutting down when the real work is done.

Real-World Use Cases
Daemon threads are used for "fire-and-forget" background support tasks that are not critical to the core business logic.

Garbage Collection (The classic example): The Java Garbage Collector runs as a Daemon thread.
It cleans up memory in the background, but it shouldn't stop the JVM from shutting down when the application finishes.

Heartbeats & Telemetry: In microservices,
an app might have a background thread sending "I am alive" pings to a service registry (like Eureka or Consul) or pushing metrics to Datadog.

Cache Eviction: A background thread that periodically scans an in-memory cache (like Redis or a local Guava cache) to delete expired or stale items.

Auto-Saving: In IDEs (like IntelliJ) or word processors, a background thread might take a snapshot of your current work every 60 seconds.

Key Differences (Cheat Sheet)

Feature                 User (Normal) Thread                                        Daemon Thread

JVM Shutdown            Prevents JVM from exiting.                                  Does not prevent JVM from exiting.
Creation                Default for all new threads.                                Must be explicitly set: thread.setDaemon(true).
Lifecycle               Runs until its run() method finishes naturally.             Runs until finished, OR until all User threads die.
Inheritance             A thread spawned by a User thread is a User thread.         A thread spawned by a Daemon thread is a Daemon thread.
 */