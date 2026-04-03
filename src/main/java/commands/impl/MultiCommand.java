package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.TransactionManager;
import serdes.RedisDeserializer;
import serdes.RedisSerializer;

import java.util.UUID;

public class MultiCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        UUID transactionId = TransactionManager.startTransaction();
        context.startTransaction(transactionId);
        return new CommandResponse(RedisSerializer.okString());
    }

    @Override
    public boolean matches(String commandName) {
        return "multi".equalsIgnoreCase(commandName);
    }
}
