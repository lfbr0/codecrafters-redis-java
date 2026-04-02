package data;

import logger.Logger;
import serdes.RedisMessage;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * MemoryManager is a singleton class that manages the actual data held by Redis
 * It provides methods to set, get, and delete keys and values
 */
public class MemoryManager {

    // Holds the key-value pairs in memory
    private static Map<String, RedisMessage> keyValueStore = new ConcurrentHashMap<>();
    // For expiry management, removes concurrently expired keys
    private static ScheduledExecutorService expiryExecutorService =
            new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "expiry-thread"));


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
}
