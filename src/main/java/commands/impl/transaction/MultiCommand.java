package commands.impl.transaction;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.TransactionManager;
import serdes.RedisSerializer;

import java.util.UUID;
import java.util.concurrent.Callable;

public class MultiCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            UUID transactionId = TransactionManager.startTransaction(context.getClientUUID());
            context.startTransaction(transactionId);
            return new CommandResponse(RedisSerializer.okString());
        };
    }

    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        return handleContext(context).call();
    }

    @Override
    public boolean matches(String commandName) {
        return "multi".equalsIgnoreCase(commandName);
    }
}
