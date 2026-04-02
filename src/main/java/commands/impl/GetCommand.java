package commands.impl;

import commands.Command;
import commands.CommandContext;
import serdes.RedisMessage;
import serdes.RedisSerializer;

public class GetCommand implements Command {
    @Override
    public void execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().isEmpty()) {
            throw new IllegalArgumentException("GET command requires at least 1 argument: key");
        }

        RedisMessage rawKey = context.getArguments().getFirst();
        if (rawKey.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("GET command requires the key to be a bulk string");
        }

        // TODO: prepare for transaction support, currently we directly get the value from memory manager
        String key = (String) rawKey.getContent();
        RedisMessage value = data.MemoryManager.get(key);

        byte[] response = value != null ?
                RedisSerializer.serialize(value) : RedisSerializer.nullBulkString();
        context.getOutputStream().write(response);
    }

    @Override
    public boolean matches(String commandName) {
        return "get".equalsIgnoreCase(commandName);
    }
}
