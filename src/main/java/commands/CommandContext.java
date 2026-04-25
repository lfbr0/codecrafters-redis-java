package commands;

import serdes.RedisMessage;

import java.io.OutputStream;
import java.util.*;

public class CommandContext {

    private final UUID clientUUID;
    private final transient OutputStream outputStream;
    private final List<RedisMessage> arguments;

    // state vars
    private boolean isInTransaction = false;
    private UUID transactionId;
    private final Set<String> subscribedChannels = Collections.synchronizedSet(new HashSet<>());

    public CommandContext(UUID clientUUID, OutputStream outputStream, List<RedisMessage> arguments) {
        this.clientUUID = clientUUID;
        this.outputStream = outputStream;
        this.arguments = arguments;
    }

    public UUID getClientUUID() {
        return clientUUID;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public List<RedisMessage> getArguments() {
        return arguments == null ? null : List.copyOf(arguments);
    }

    public boolean isInTransaction() {
        return isInTransaction;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    public void startTransaction(UUID transactionId) {
        this.isInTransaction = true;
        this.transactionId = transactionId;
    }

    public UUID endTransaction() {
        UUID oldTransactionId = UUID.fromString(this.transactionId.toString());
        this.isInTransaction = false;
        this.transactionId = null;
        return oldTransactionId;
    }

    public int subscribeToChannel(String channel) {
        synchronized (subscribedChannels) {
            subscribedChannels.add(channel);
            return subscribedChannels.size();
        }
    }

    @Override
    public String toString() {
        return "CommandContext{" +
                "transactionId=" + transactionId +
                ", isInTransaction=" + isInTransaction +
                ", arguments=" + arguments +
                '}';
    }
}
