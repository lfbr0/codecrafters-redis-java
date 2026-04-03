package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.TransactionManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.io.IOException;

public class EchoCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (context.getArguments().isEmpty()) {
            throw new IllegalArgumentException("ECHO command requires at least one argument");
        }

        RedisMessage firstArg = context.getArguments().getFirst();
        if (firstArg.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("ECHO command argument must be a bulk string");
        }

        if (context.isInTransaction()) {
            TransactionManager.addOperation(context.getTransactionId(), () -> echo(firstArg));
            return CommandResponse.queued();
        } else {
            return new CommandResponse(echo(firstArg));
        }
    }

    private byte[] echo(RedisMessage firstArg) {
        return RedisSerializer.serialize(firstArg);
    }

    @Override
    public boolean matches(String commandName) {
        return "echo".equalsIgnoreCase(commandName);
    }
}
