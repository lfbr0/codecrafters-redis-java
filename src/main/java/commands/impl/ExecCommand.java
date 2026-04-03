package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.TransactionManager;
import logger.Logger;
import serdes.RedisDeserializer;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.ArrayList;
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
        List<RedisMessage> messages = new ArrayList<>(messagesRaw.size());
        for (byte[] messageRaw : messagesRaw) {
            messages.add(RedisDeserializer.deserialize(messageRaw));
        }

        return new CommandResponse(RedisSerializer.list(messages));
    }

    @Override
    public boolean matches(String commandName) {
        return "exec".equalsIgnoreCase(commandName);
    }
}
