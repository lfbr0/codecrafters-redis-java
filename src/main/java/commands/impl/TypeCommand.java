package commands.impl;

import commands.Command;
import commands.CommandContext;
import data.MemoryManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

public class TypeCommand implements Command {
    @Override
    public void execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().isEmpty()) {
            throw new IllegalArgumentException("Missing key argument for TYPE command");
        }

        RedisMessage keyRaw = context.getArguments().getFirst();
        if (keyRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("Expected a bulk string for key argument, but got: " + keyRaw.getType());
        }

        String key = (String) keyRaw.getContent();

        // TODO: prepare for transactions
        String type = MemoryManager.type(key);
        context.getOutputStream().write(RedisSerializer.simpleString(type == null ? "none" : type));
    }

    @Override
    public boolean matches(String commandName) {
        return "type".equalsIgnoreCase(commandName);
    }
}
