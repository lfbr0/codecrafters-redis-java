package commands.impl;

import commands.Command;
import commands.CommandContext;
import data.MemoryManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

public class LlenCommand implements Command {
    @Override
    public void execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().isEmpty()) {
            throw new IllegalArgumentException("LLEN command requires a key argument");
        }

        RedisMessage rawKey = context.getArguments().getFirst();
        if (rawKey == null || rawKey.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("LLEN command requires a bulk string key argument");
        }

        // TODO: prepare for transactions
        String key = (String) rawKey.getContent();
        context.getOutputStream().write(RedisSerializer.integer(MemoryManager.lengthOfList(key)));
    }

    @Override
    public boolean matches(String commandName) {
        return "llen".equalsIgnoreCase(commandName);
    }
}
