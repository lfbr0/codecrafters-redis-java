package commands.impl.transaction;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.TransactionManager;
import logger.Logger;
import serdes.RedisSerializer;

import java.util.List;
import java.util.UUID;

public class ExecCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (!context.isInTransaction()) {
            return new CommandResponse(RedisSerializer.error("ERR EXEC without MULTI"));
        }

        Logger.info("Exiting transaction mode " + context.getTransactionId());
        UUID transactionId = context.endTransaction();

        // execute all transactions
        List<byte[]> messagesRaw = TransactionManager.commitTransaction(transactionId);
        return new CommandResponse(RedisSerializer.listRaw(messagesRaw));
    }

    @Override
    public boolean matches(String commandName) {
        return "exec".equalsIgnoreCase(commandName);
    }
}
