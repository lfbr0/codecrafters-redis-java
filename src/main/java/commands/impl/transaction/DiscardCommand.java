package commands.impl.transaction;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.TransactionManager;
import serdes.RedisSerializer;

import java.util.UUID;
import java.util.concurrent.Callable;

public class DiscardCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (!context.isInTransaction()) {
                return new CommandResponse(RedisSerializer.error("ERR DISCARD without MULTI"));
            }

            UUID discardedTransactionId = context.endTransaction();
            TransactionManager.abortTransaction(discardedTransactionId);

            return new CommandResponse(RedisSerializer.okString());
        };
    }

    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        return handleContext(context).call();
    }

    @Override
    public boolean matches(String commandName) {
        return "discard".equalsIgnoreCase(commandName);
    }
}
