package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.TransactionManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.concurrent.Callable;

public class EchoCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments().isEmpty()) {
                throw new IllegalArgumentException("ECHO command requires at least one argument");
            }

            RedisMessage firstArg = context.getArguments().getFirst();
            if (firstArg.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                throw new IllegalArgumentException("ECHO command argument must be a bulk string");
            }

            return new CommandResponse(RedisSerializer.serialize(firstArg));
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "echo".equalsIgnoreCase(commandName);
    }
}
