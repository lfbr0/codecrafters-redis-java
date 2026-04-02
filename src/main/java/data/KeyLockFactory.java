package data;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KeyLockFactory {

    private KeyLockFactory() {}
    private static final Map<String, ReentrantReadWriteLock> lockMap = new ConcurrentHashMap<>();

    public static ReentrantReadWriteLock getLock(String key) {
        return lockMap.computeIfAbsent(key, k -> new ReentrantReadWriteLock());
    }

    public static void removeLock(String key) {
        lockMap.remove(key);
    }

}
