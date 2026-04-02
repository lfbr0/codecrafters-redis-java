package commands.impl;

import commands.Command;
import commands.CommandContext;
import serdes.RedisDeserializer;
import serdes.RedisMessage;
import serdes.RedisSerializer;

public class EchoCommand implements Command {
    @Override
    public void execute(CommandContext context) throws Exception {
        if (context.getArguments().isEmpty()) {
            throw new IllegalArgumentException("ECHO command requires at least one argument");
        }

        RedisMessage firstArg = context.getArguments().getFirst();
        if (firstArg.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("ECHO command argument must be a bulk string");
        }

        context.getOutputStream().write(RedisSerializer.serialize(firstArg));
    }

    @Override
    public boolean matches(String commandName) {
        return "echo".equalsIgnoreCase(commandName);
    }
}
