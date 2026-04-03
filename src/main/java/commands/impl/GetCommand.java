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

        if (value == null) {
            context.getOutputStream().write(RedisSerializer.nullBulkString());
            return;
        } else if (value.getType() == RedisMessage.RedisMessageType.INTEGER) {
            Integer valueInt = (Integer) value.getContent();
            // If the value is an integer, we need to serialize it as a bulk string for the GET command response
            context.getOutputStream().write(RedisSerializer.bulkString(valueInt.toString()));
        } else {
            // For other types, we can directly serialize the value
            context.getOutputStream().write(RedisSerializer.serialize(value));
        }
    }

    @Override
    public boolean matches(String commandName) {
        return "get".equalsIgnoreCase(commandName);
    }
}
