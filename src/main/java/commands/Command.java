package commands;

import data.TransactionManager;

import java.util.concurrent.Callable;

public interface Command {

    default CommandResponse execute(CommandContext context) throws Exception {
        Callable<CommandResponse> operation = handleContext(context);
        if (context.isInTransaction()) {
            TransactionManager.addOperation(context.getTransactionId(), () -> operation.call().getResponseBytes());
            return CommandResponse.queued();
        }
        return operation.call();
    }

    Callable<CommandResponse> handleContext(CommandContext context);
    boolean matches(String commandName);

    // by default, command is not write (doesn't write into memory)
    default boolean isWriteCommand() {
        return false;
    }

    // by default, command is not subscriber mode allowed
    default boolean isSubscriberModeAllowedCommand() {
        return false;
    }
}
