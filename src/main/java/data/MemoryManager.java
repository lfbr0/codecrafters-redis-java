package data;

import logger.Logger;
import serdes.RedisMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
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
