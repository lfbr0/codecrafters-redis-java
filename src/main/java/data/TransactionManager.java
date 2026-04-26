package data;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionManager {

    private TransactionManager() {}

    private final static Map<UUID, List<Callable<byte[]>>> transactionToOperations = new ConcurrentHashMap<>();
    private final static Map<UUID, Set<String>> clientToWatchedKeys = new ConcurrentHashMap<>();
    private final static Map<String, Long> keyModCount = new ConcurrentHashMap<>();
    private final static Set<UUID> dirtyClients = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public static UUID startTransaction() {
        UUID transactionUUID = UUID.randomUUID();
        transactionToOperations.put(transactionUUID, new LinkedList<>());
        return transactionUUID;
    }

    public static void abortTransaction(UUID discardedTransactionId) {
        transactionToOperations.remove(discardedTransactionId);
    }

    public static void addOperation(UUID transactionId, Callable<byte[]> operation) {
        transactionToOperations.computeIfPresent(transactionId, (id, operations) -> {
            operations.add(operation);
            return operations;
        });
    }

    public static List<byte[]> commitTransaction(UUID transactionId) {
        List<Callable<byte[]>> callables = transactionToOperations.remove(transactionId);

        if (callables == null) {
            return List.of();
        }

        return callables.stream()
                .map(redisMessageCallable -> {
                    try {
                        return redisMessageCallable.call();
                    } catch (Exception e) {
                        // In a real implementation, we would want to handle this more gracefully
                        throw new RuntimeException("Failed to execute transaction operation: " + e.getMessage(), e);
                    }
                })
                .toList();
    }

    public static void watchKey(String key, UUID clientUUID) {
        clientToWatchedKeys.compute(clientUUID, (uuid, previousWatchedKeys) -> {
            Set<String> watchedKeys = (previousWatchedKeys == null) ? new HashSet<>() : previousWatchedKeys;
            watchedKeys.add(key);
            return watchedKeys;
        });
    }

    public static void notifyKeyModified(String key) {
        keyModCount.compute(key, (k, v) -> (v == null) ? 1L : v + 1);
        clientToWatchedKeys.forEach((clientUUID, watchedKeys) -> {
            if (watchedKeys.contains(key)) {
                dirtyClients.add(clientUUID);
            }
        });
    }

    public static boolean isTransactionAborted(UUID clientUUID) {
        return dirtyClients.contains(clientUUID);
    }

    public static void clearWatchedKeys(UUID clientUUID) {
        clientToWatchedKeys.remove(clientUUID);
        dirtyClients.remove(clientUUID);
    }

}
