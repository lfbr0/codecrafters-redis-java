package commands;

import serdes.RedisMessage;

import java.io.OutputStream;
import java.util.List;
import java.util.UUID;

public class CommandContext {

    private final transient OutputStream outputStream;
    private final List<RedisMessage> arguments;

    // state vars
    private boolean isInTransaction = false;
    private UUID transactionId;

    public CommandContext(OutputStream outputStream, List<RedisMessage> arguments) {
        this.outputStream = outputStream;
        this.arguments = arguments;
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

    @Override
    public String toString() {
        return "CommandContext{" +
                "transactionId=" + transactionId +
                ", isInTransaction=" + isInTransaction +
                ", arguments=" + arguments +
                '}';
    }
}
