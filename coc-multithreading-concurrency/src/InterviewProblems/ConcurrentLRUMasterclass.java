package InterviewProblems;

/**
 * ============================================================================
 * 1. HEADER & PROBLEM CONTEXT
 * ============================================================================
 * Problem Statement: Design a Concurrent LRU Cache
 *
 * Design and implement a data structure for a Least Recently Used (LRU) cache
 * that supports concurrent reads and writes from multiple threads in a thread-safe
 * manner without corrupting the internal data structures.
 *
 * Implement the `ConcurrentLRUCache` class:
 * - `ConcurrentLRUCache(int capacity)` Initialize the LRU cache with positive size capacity.
 * - `int get(int key)` Return the value of the key if the key exists, otherwise return -1.
 * - `void put(int key, int value)` Update the value of the key if the key exists.
 *   Otherwise, add the key-value pair to the cache. If the number of keys exceeds
 *   the capacity from this operation, evict the least recently used key.
 *
 * Constraints:
 * - 1 <= capacity <= 3000
 * - 0 <= key <= 10000
 * - 0 <= value <= 10^5
 * - Must be completely thread-safe against concurrent access.
 * - Expected Time Complexity: O(1) for both get and put.
 *
 * Input/Output Formats:
 * Input: Concurrent calls to put() and get().
 * Output: Consistent cache state adhering to the capacity limit, accurately evicting
 * the least recently used elements despite race conditions.
 * ============================================================================
 */

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class ConcurrentLRUMasterclass {

    /**
     * ============================================================================
     * 2.2 PROGRESSIVE IMPLEMENTATION ROADMAP (Non-DP)
     * ============================================================================
     * Phase 1: Optimal Approach - ConcurrentHashMap + Doubly Linked List + ReentrantLock
     *
     * Intuition:
     * A standard LRU cache uses a HashMap for O(1) lookups and a Doubly Linked List
     * (DLL) for O(1) ordering and evictions.
     *
     * In a concurrent environment, simply using a `ConcurrentHashMap` is not enough
     * because moving nodes around in the DLL involves modifying multiple pointers
     * (prev/next). If two threads modify the DLL simultaneously, the pointers will
     * become corrupted and form cycles or orphan nodes.
     *
     * Furthermore, a `ReadWriteLock` is ineffective here because in an LRU cache,
     * even a `get()` operation is fundamentally a "Write" operation on the underlying
     * DLL (it must move the accessed node to the head).
     *
     * Optimal strategy for an interview:
     * 1. Use `ConcurrentHashMap` for lock-free, thread-safe value lookups.
     * 2. Use a single `ReentrantLock` STRICTLY and EXCLUSIVELY to protect the DLL
     *    pointer manipulations. This keeps the critical section extremely small.
     *
     * Complexity Analysis:
     * - Time Complexity:
     *   - get(): O(1) mostly lock-free lookup, O(1) locked DLL update.
     *   - put(): O(1) locked node insertion/eviction.
     * - Space Complexity: O(capacity) for the Map and DLL.
     * ============================================================================
     */
    static class OptimalConcurrentLRUCache {

        class Node {
            int key, value;
            Node prev, next;
            Node(int key, int value) {
                this.key = key;
                this.value = value;
            }
        }

        private final int capacity;
        private final ConcurrentHashMap<Integer, Node> map;
        private final Node head, tail;

        // Lock specifically for Doubly Linked List mutations
        private final ReentrantLock listLock;

        public OptimalConcurrentLRUCache(int capacity) {
            this.capacity = capacity;
            this.map = new ConcurrentHashMap<>(capacity);
            this.listLock = new ReentrantLock();

            // Dummy head and tail to avoid null checks during DLL operations
            head = new Node(-1, -1);
            tail = new Node(-1, -1);
            head.next = tail;
            tail.prev = head;
        }

        public int get(int key) {
            // Lock-free read from the map
            Node node = map.get(key);
            if (node == null) {
                return -1;
            }

            // Lock required because get() modifies the DLL structure
            listLock.lock();
            try {
                moveToHead(node);
            } finally {
                listLock.unlock();
            }

            return node.value;
        }

        public void put(int key, int value) {
            listLock.lock();
            try {
                Node node = map.get(key);
                if (node != null) {
                    // Update existing node
                    node.value = value;
                    moveToHead(node);
                } else {
                    // Insert new node
                    Node newNode = new Node(key, value);
                    map.put(key, newNode);
                    addToHead(newNode);

                    // Evict if over capacity
                    if (map.size() > capacity) {
                        Node lru = removeTail();
                        map.remove(lru.key); // Safe to call inside lock
                    }
                }
            } finally {
                listLock.unlock();
            }
        }

        // --- DLL Helper Methods (MUST be called while holding listLock) ---

        private void addToHead(Node node) {
            node.prev = head;
            node.next = head.next;
            head.next.prev = node;
            head.next = node;
        }

        private void removeNode(Node node) {
            node.prev.next = node.next;
            node.next.prev = node.prev;
        }

        private void moveToHead(Node node) {
            removeNode(node);
            addToHead(node);
        }

        private Node removeTail() {
            Node lru = tail.prev;
            removeNode(lru);
            return lru;
        }
    }

    /**
     * ============================================================================
     * Phase 2: Brute Force Approach - Synchronized LinkedHashMap
     *
     * Intuition:
     * Java's `LinkedHashMap` natively supports LRU eviction if you pass `true` to
     * its `accessOrder` constructor argument and override `removeEldestEntry`.
     * To make it thread-safe, we can wrap the entire map in `Collections.synchronizedMap`.
     *
     * Why is this considered Brute Force for a Senior Interview?
     * Because `Collections.synchronizedMap` places a coarse-grained intrinsic lock
     * (monitor lock) on the entire map object. EVERY get() and put() completely
     * blocks all other threads from touching the cache, destroying true concurrency.
     *
     * Complexity Analysis:
     * - Time Complexity: O(1) theoretically, but O(N) practically under high thread
     *   contention due to the coarse-grained lock blocking concurrent threads.
     * - Space Complexity: O(capacity).
     * ============================================================================
     */
    static class BruteForceConcurrentLRUCache {
        private final Map<Integer, Integer> cache;

        public BruteForceConcurrentLRUCache(int capacity) {
            // true = access order (LRU mode)
            this.cache = Collections.synchronizedMap(
                    new LinkedHashMap<Integer, Integer>(capacity, 0.75f, true) {
                        @Override
                        protected boolean removeEldestEntry(Map.Entry<Integer, Integer> eldest) {
                            return size() > capacity;
                        }
                    }
            );
        }

        public int get(int key) {
            return cache.getOrDefault(key, -1);
        }

        public void put(int key, int value) {
            cache.put(key, value);
        }
    }

    /**
     * ============================================================================
     * Phase 3: Alternative Approaches (Advanced Architectural Patterns)
     *
     * 1. Lock Striping (Segmented LRU):
     *    Instead of one global lock for the DLL, divide the cache into `N` segments
     *    (like the original pre-Java-8 ConcurrentHashMap). Each segment acts as its
     *    own independent LRU cache with its own lock. This allows `N` threads to
     *    operate concurrently, drastically reducing lock contention.
     *
     * 2. Non-blocking/Eventual Consistency LRU (Caffeine Cache pattern):
     *    Instead of modifying the DLL inline during a `get()`, record the access
     *    event in a highly concurrent lock-free Ring Buffer. A background thread
     *    (or amortized maintenance during writes) processes this buffer asynchronously
     *    to update the DLL. This makes `get()` strictly lock-free.
     * ============================================================================
     */

    /**
     * ============================================================================
     * 4. TESTING SUITE
     * ============================================================================
     */
    public static void main(String[] args) throws InterruptedException {
        System.out.println("--- Starting Concurrent LRU Cache Test ---");

        final int CAPACITY = 3;
        OptimalConcurrentLRUCache cache = new OptimalConcurrentLRUCache(CAPACITY);

        // Pre-populate
        cache.put(1, 10);
        cache.put(2, 20);
        cache.put(3, 30);
        System.out.println("Initial Cache State logic complete.");

        // Create multiple threads to bombard the cache simultaneously
        Runnable readTask = () -> {
            for (int i = 0; i < 100; i++) {
                cache.get(1); // Keeps key 1 fresh
            }
        };

        Runnable writeTask = () -> {
            for (int i = 4; i <= 10; i++) {
                cache.put(i, i * 10);
                try { Thread.sleep(2); } catch (InterruptedException ignored) {}
            }
        };

        Thread t1 = new Thread(readTask, "ReaderThread");
        Thread t2 = new Thread(writeTask, "WriterThread");

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        // After all concurrent operations, verify the state.
        // Because ReaderThread constantly refreshed Key 1, it should SURVIVE the evictions
        // caused by WriterThread adding keys 4 through 10.
        // The cache (capacity 3) should contain: {1, 9, 10}

        System.out.println("\nFinal Verification:");
        System.out.println("Key 1 (Should be 10): " + cache.get(1));
        System.out.println("Key 2 (Should be evicted, -1): " + cache.get(2));
        System.out.println("Key 3 (Should be evicted, -1): " + cache.get(3));
        System.out.println("Key 8 (Should be evicted, -1): " + cache.get(8));
        System.out.println("Key 9 (Should be 90): " + cache.get(9));
        System.out.println("Key 10 (Should be 100): " + cache.get(10));

        System.out.println("\nTest Complete.");
    }
}