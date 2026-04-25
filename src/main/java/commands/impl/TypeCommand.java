package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import data.TransactionManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.concurrent.Callable;

public class TypeCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().isEmpty()) {
                throw new IllegalArgumentException("Missing key argument for TYPE command");
            }

            RedisMessage keyRaw = context.getArguments().getFirst();
            if (keyRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                throw new IllegalArgumentException("Expected a bulk string for key argument, but got: " + keyRaw.getType());
            }

            String key = (String) keyRaw.getContent();
            String type = MemoryManager.type(key);

            return new CommandResponse(RedisSerializer.simpleString(type == null ? "none" : type));
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "type".equalsIgnoreCase(commandName);
    }
}
