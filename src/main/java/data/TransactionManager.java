package data;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TransactionManager {

    private TransactionManager() {}

    private static Map<UUID, List<Runnable>> transactionToOperations = new ConcurrentHashMap<>();

    public static UUID startTransaction() {
        UUID uuid = UUID.randomUUID();
        transactionToOperations.put(uuid, new LinkedList<>());
        return uuid;
    }

    public static void addOperation(UUID transactionId, Runnable operation) {
        transactionToOperations.computeIfPresent(transactionId, (id, operations) -> {
            operations.add(operation);
            return operations;
        });
    }

    public static void commitTransaction(UUID transactionId) {
        List<Runnable> operations = transactionToOperations.remove(transactionId);
        if (operations != null) {
            for (Runnable operation : operations) {
                operation.run();
            }
        }
    }

}
