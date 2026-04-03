package data;

import serdes.RedisMessage;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionManager {

    private TransactionManager() {}

    private final static Map<UUID, List<Callable<byte[]>>> transactionToOperations = new ConcurrentHashMap<>();

    public static UUID startTransaction() {
        UUID uuid = UUID.randomUUID();
        transactionToOperations.put(uuid, new LinkedList<>());
        return uuid;
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

}
