package commands.impl;

import commands.Command;
import commands.CommandContext;
import data.MemoryManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class IncrCommand implements Command {
    @Override
    public void execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().size() != 1) {
            throw new IllegalArgumentException("INCR command requires exactly 1 argument");
        }

        RedisMessage keyRaw = context.getArguments().getFirst();
        if (keyRaw.getType() != BULK_STRING) {
            throw new IllegalArgumentException("INCR command requires a bulk string argument");
        }

        String key = (String) keyRaw.getContent();

        // TODO: prepare for transaction support
        try {
            int result = MemoryManager.increment(key);
            context.getOutputStream().write(RedisSerializer.integer(result));
        } catch (IllegalArgumentException e) {
            context.getOutputStream().write(RedisSerializer.error(e.getMessage()));
        }
    }

    @Override
    public boolean matches(String commandName) {
        return "incr".equalsIgnoreCase(commandName);
    }
}
