package data;

import logger.Logger;
import serdes.RedisMessage;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * MemoryManager is a singleton class that manages the actual data held by Redis
 * It provides methods to set, get, and delete keys and values
 */
public class MemoryManager {

    // Holds the key-value pairs in memory
    private static final Map<String, RedisMessage> keyValueStore = new ConcurrentHashMap<>();
    // For expiry management, removes concurrently expired keys
    private static final ScheduledExecutorService expiryExecutorService =
            new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "expiry-thread"));
    // For lists in rpush
    private static final Map<String, List<RedisMessage>> listStore = new ConcurrentHashMap<>();


    /**
     * Sets the value for the given key in the memory store with an expiration duration.
     * If the key already exists, it will be overwritten
     * @param key the key to set
     * @param value the value to associate with the key
     * @param expireDuration the duration after which the key should expire and be removed from the memory store
     */
    public static void set(String key, RedisMessage value, Duration expireDuration) {
        set(key, value);

        // schedule a task to remove the key after the expiration duration
        expiryExecutorService.schedule(() -> {
            Logger.info("Expiring key: " + key);
            delete(key);
        }, expireDuration.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the value for the given key in the memory store. If the key already exists, it will be overwritten.
     * @param key the key to set
     * @param value the value to associate with the key
     */
    public static void set(String key, RedisMessage value) {
        ReentrantReadWriteLock.WriteLock writeLock = KeyLockFactory
                .getLock("data[" + key + "]")
                .writeLock();

        try {
            writeLock.lock();
            Logger.info("Setting key: " + key + " to value: " + value);
            keyValueStore.put(key, value);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Deletes the given key from the memory store. If the key does not exist, this operation has no effect.
     * @param key the key to delete
     */
    public static void delete(String key) {
        ReentrantReadWriteLock.WriteLock writeLock = KeyLockFactory
                .getLock("data[" + key + "]")
                .writeLock();

        try {
            writeLock.lock();
            Logger.info("Deleting key: " + key);
            keyValueStore.remove(key);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Retrieves the value associated with the given key from the memory store. If the key does not exist, it returns null.
     * @param key the key to retrieve
     * @return the value associated with the key, or null if the key does not exist
     */
    public static RedisMessage get(String key) {
        ReentrantReadWriteLock.ReadLock readLock = KeyLockFactory
                .getLock("data[" + key + "]")
                .readLock();

        try {
            readLock.lock();
            Logger.info("Getting key: " + key);
            return keyValueStore.get(key);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Pushes the given values to the end of the list stored at listKey. If the list does not exist, it will be created.
     * @param listKey the key of the list to push values to
     * @param values the values to push to the list
     * @return the length of the list after the push operation
     */
    public static int appendToList(String listKey, RedisMessage ... values) {
        ReentrantReadWriteLock.WriteLock writeLock = KeyLockFactory
                .getLock("list[" + listKey + "]")
                .writeLock();

        try {
            writeLock.lock();
            Logger.info("Pushing values: " + List.of(values) + " to list: " + listKey);
            List<RedisMessage> list = listStore
                    .computeIfAbsent(listKey, k -> new CopyOnWriteArrayList<>());
            list.addAll(Arrays.asList(values));
            return list.size();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Retrieves a range of elements from the list stored at listKey. The range is specified by the start and end indices.
     * @param listKey the key of the list to retrieve elements from
     * @param start the starting index of the range (inclusive)
     * @param end the ending index of the range (inclusive)
     * @return an array of RedisMessage containing the elements in the specified range, or empty list
     */
    public static List<RedisMessage> rangeFromList(String listKey, int start, int end) {
        ReentrantReadWriteLock.ReadLock readLock = KeyLockFactory
                .getLock("list[" + listKey + "]")
                .readLock();

        try {
            readLock.lock();
            Logger.info("Getting list from list: " + listKey);

            List<RedisMessage> list = listStore.get(listKey);
            if (list == null) {
                return List.of();
            }

            if (start >= list.size()) {
                return List.of();
            } else if (end >= list.size()) {
                end = list.size() - 1;
            } else if (end < start) {
                return List.of();
            }

            return list.subList(start, end + 1);
        } finally {
            readLock.unlock();
        }
    }
}
