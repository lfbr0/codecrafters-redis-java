package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import data.TransactionManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

public class GetCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().isEmpty()) {
            throw new IllegalArgumentException("GET command requires at least 1 argument: key");
        }

        RedisMessage rawKey = context.getArguments().getFirst();
        if (rawKey.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("GET command requires the key to be a bulk string");
        }

        String key = (String) rawKey.getContent();

        if (context.isInTransaction()) {
            TransactionManager.addOperation(context.getTransactionId(), () -> get(key));
            return CommandResponse.queued();
        } else {
            return new CommandResponse(get(key));
        }
    }

    private byte[] get(String key) {
        RedisMessage value = MemoryManager.get(key);

        if (value == null) {
            return RedisSerializer.nullBulkString();
        } else if (value.getType() == RedisMessage.RedisMessageType.INTEGER) {
            Integer valueInt = (Integer) value.getContent();
            // If the value is an integer, we need to serialize it as a bulk string for the GET command response
            return RedisSerializer.bulkString(valueInt.toString());
        } else {
            // For other types, we can directly serialize the value
            return RedisSerializer.serialize(value);
        }
    }

    @Override
    public boolean matches(String commandName) {
        return "get".equalsIgnoreCase(commandName);
    }
}
