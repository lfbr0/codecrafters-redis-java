package commands.impl;

import commands.Command;
import commands.CommandContext;
import data.MemoryManager;
import data.TransactionManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

public class SetCommand implements Command {
    @Override
    public void execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().size() < 2) {
            throw new IllegalArgumentException("SET command requires at least 2 arguments: key and value");
        }

        RedisMessage keyRaw = context.getArguments().getFirst();
        if (keyRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("SET command requires the key to be a bulk string");
        }

        // TODO: prepare for transaction support, currently we directly set the value in memory manager
        String key = (String) keyRaw.getContent();
        RedisMessage value = context.getArguments().get(1);
        MemoryManager.set(key, value);
        // respond with OK
        context.getOutputStream().write(RedisSerializer.okString());
    }

    @Override
    public boolean matches(String commandName) {
        return "set".equalsIgnoreCase(commandName);
    }
}
