package data;

import logger.Logger;
import serdes.RedisMessage;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.lang.String.format;

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
    // For lists subscription
    private static final Map<String, Queue<SynchronousQueue<RedisMessage>>> listPopSubs = new ConcurrentHashMap<>();

    // For sorted sets
    private static final Map<String, RedisSortedSet> sortedSetStore = new ConcurrentHashMap<>();

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
            // fix memory leak for lock factory
            KeyLockFactory.removeLock("data[" + key + "]");
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
     * Increments the integer value associated with the given key by 1.
     * If the key does not exist, it will be created with a value of 1.
     * @param key the key to increment
     * @return the new value after incrementing
     * @throws IllegalArgumentException if the current value associated with the key is not an integer
     */
    public static int increment(String key) throws IllegalArgumentException {
        ReentrantReadWriteLock.WriteLock writeLock = KeyLockFactory
                .getLock("data[" + key + "]")
                .writeLock();

        try {
            writeLock.lock();
            Logger.info("Incrementing key: " + key);
            RedisMessage currentValue = keyValueStore.get(key);

            int nextValue;
            if (currentValue == null || currentValue.getContent() == null) {
                nextValue = 1;
            } else {
                Object rawContent = currentValue.getContent();

                try {
                    switch (currentValue.getType()) {
                        case INTEGER:
                            if (rawContent instanceof Integer intValue) {
                                nextValue = intValue + 1;
                            } else if (rawContent instanceof Long longValue) {
                                nextValue = Math.toIntExact(longValue + 1);
                            } else {
                                nextValue = Integer.parseInt(rawContent.toString()) + 1;
                            }
                            break;

                        case BULK_STRING:
                        case SIMPLE_STRING:
                            nextValue = Integer.parseInt(rawContent.toString()) + 1;
                            break;

                        default:
                            throw new IllegalArgumentException("ERR value is not an integer or out of range");
                    }
                } catch (NumberFormatException | ArithmeticException e) {
                    throw new IllegalArgumentException("ERR value is not an integer or out of range");
                }
            }

            RedisMessage newValue = new RedisMessage();
            newValue.setType(RedisMessage.RedisMessageType.INTEGER);
            newValue.setContent(nextValue);

            keyValueStore.put(key, newValue);
            return nextValue;
        } finally {
            writeLock.unlock();
        }
    }

    // LIST OPERATIONS

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
            popFromListToSubs(listKey);
        }
    }

    /**
     * Pushes the given values to the beginning of the list stored at listKey. If the list does not exist, it will be created.
     * @param listKey the key of the list to push values to
     * @param values the values to push to the list
     * @return the length of the list after the push operation
     */
    public static int prependToList(String listKey, RedisMessage ... values) {
        ReentrantReadWriteLock.WriteLock writeLock = KeyLockFactory
                .getLock("list[" + listKey + "]")
                .writeLock();

        try {
            writeLock.lock();
            Logger.info("Prepending values: " + List.of(values) + " to list: " + listKey);
            List<RedisMessage> list = listStore
                    .computeIfAbsent(listKey, k -> new CopyOnWriteArrayList<>());
            for (RedisMessage value : values) {
                list.addFirst(value);
            }
            return list.size();
        } finally {
            writeLock.unlock();
            popFromListToSubs(listKey);
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

            // if start or end negative, it's inverse from start
            start = start < 0 ? list.size() + start : start;
            end = end < 0 ? list.size() + end : end;

            if (start >= list.size()) {
                return List.of();
            } else if (end >= list.size()) {
                end = list.size() - 1;
            } else if (end < start) {
                return List.of();
            } else if (start < 0) {
                start = 0;
            }

            Logger.info("Getting list from list: " + listKey + " with start: " + start + " and end: " + end);
            return list.subList(start, end + 1);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the length of the list stored at listKey. If the list does not exist, it returns 0.
     * @param key the key of the list to get the length of
     * @return the length of the list, or 0 if the list does not exist
     */
    public static int lengthOfList(String key) {
        ReentrantReadWriteLock.ReadLock readLock = KeyLockFactory
                .getLock("list[" + key + "]")
                .readLock();

        try {
            readLock.lock();
            Logger.info("Getting length of list: " + key);
            List<RedisMessage> list = listStore.get(key);
            return list == null ? 0 : list.size();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Pops the specified number of elements from the beginning of the list stored at key.
     * If the list does not exist or is empty, it returns an empty list.
     * @param key the key of the list to pop elements from
     * @param popCount the number of elements to pop from the list
     * @return a list of RedisMessage containing the popped elements, or an empty list if the list does not exist or is empty
     */
    public static List<RedisMessage> popFromList(String key, int popCount) {
        Logger.info("Popping " + popCount + " elements from list: " + key);
        ReentrantReadWriteLock.ReadLock readLock = KeyLockFactory
                .getLock("list[" + key + "]")
                .readLock();

        try {
            readLock.lock();

            List<RedisMessage> list = listStore.get(key);
            if (list == null || list.isEmpty()) {
                return List.of();
            }

            List<RedisMessage> result = new ArrayList<>(popCount);
            while (popCount-- > 0) {
                result.add(list.removeFirst());
            }
            return result;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Registers a SynchronousQueue to be notified when an element is popped from the list stored at listKey.
     * @param listKey the key of the list to subscribe to for pop notifications
     * @param queue queue to place popped value
     */
    public static void blockingPopFromList(String listKey, SynchronousQueue<RedisMessage> queue) {
        Logger.info("Blocking pop from list: " + listKey);
        listPopSubs
                .computeIfAbsent(listKey, k -> new ConcurrentLinkedQueue<>())
                .add(queue);
    }

    /**
     * Pop from list to longest waiting subscriber, if any.
     * This method is called after every push to the list
     * to ensure that waiting subscribers are notified as soon as possible.
     * @param listKey the key of the list to pop from and notify subscribers about
     */
    private static void popFromListToSubs(String listKey) {
        ReentrantReadWriteLock.WriteLock writeLock = KeyLockFactory
                .getLock("list[" + listKey + "]")
                .writeLock();

        try {
            writeLock.lock();

            List<RedisMessage> list = listStore.get(listKey);
            if (list == null || list.isEmpty()) {
                return;
            }

            Queue<SynchronousQueue<RedisMessage>> subs = listPopSubs.get(listKey);
            if (subs == null || subs.isEmpty()) {
                return;
            }

            RedisMessage poppedValue = list.removeFirst();
            SynchronousQueue<RedisMessage> sub = subs.poll();
            if (sub != null) {
                sub.offer(poppedValue);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     *
     * @param zSetKey
     * @param zSetMember
     * @param zSetScore
     * @return
     */
    public static boolean addToSortedSet(String zSetKey, String zSetMember, double zSetScore) {
        ReentrantReadWriteLock.WriteLock writeLock = KeyLockFactory
                .getLock("sortedset[" + zSetKey + "]")
                .writeLock();
        boolean appended = false;

        try {
            writeLock.lock();
            appended = sortedSetStore
                    .computeIfAbsent(zSetKey, key -> new RedisSortedSet())
                    .add(new RedisSortedSet.RedisSortedSetEntry(zSetMember, zSetScore));
        } catch (Exception ex) {
            Logger.error(format("Error appending to set[%s] member[%s] rank[%f]\n", zSetKey, zSetMember, zSetScore), ex);
        } finally {
            writeLock.unlock();
        }

        return appended;
    }

    /**
     * Returns the type of the value stored at the given key. If the key does not exist, it returns null.
     * @param key the key to check the type of
     * @return the type of the value stored at the key ("string", "list", etc.), or null if the key does not exist
     */
    public static String type(String key) {
        // no locks since we just want to verify existance
        if (keyValueStore.containsKey(key)) {
            return "string";
        } else if (listStore.containsKey(key)) {
            return "list";
        } else if (sortedSetStore.containsKey(key)) {
            return "sortedset";
        } else {
            return null;
        }
    }
}
