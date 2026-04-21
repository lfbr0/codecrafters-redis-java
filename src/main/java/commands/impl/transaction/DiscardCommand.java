package commands.impl.transaction;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.TransactionManager;
import serdes.RedisSerializer;

import java.util.UUID;

public class DiscardCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (!context.isInTransaction()) {
            return new CommandResponse(RedisSerializer.error("ERR DISCARD without MULTI"));
        }

        UUID discardedTransactionId = context.endTransaction();
        TransactionManager.abortTransaction(discardedTransactionId);

        return new CommandResponse(RedisSerializer.okString());
    }

    @Override
    public boolean matches(String commandName) {
        return "discard".equalsIgnoreCase(commandName);
    }
}
