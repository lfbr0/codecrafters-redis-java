package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.TransactionManager;
import serdes.RedisSerializer;

import java.io.IOException;
import java.util.concurrent.Callable;

public class PingCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        Callable<byte[]> task = () -> RedisSerializer.simpleString("PONG");

        if (context.isInTransaction()) {
            TransactionManager.addOperation(context.getTransactionId(), task);
            return CommandResponse.queued();
        }

        return new CommandResponse(task.call());
    }

    @Override
    public boolean matches(String commandName) {
        return "ping".equalsIgnoreCase(commandName);
    }
}
