package commands.impl;

import commands.Command;
import commands.CommandContext;
import data.MemoryManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.List;

public class LpopCommand implements Command {
    @Override
    public void execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().isEmpty()) {
            throw new IllegalArgumentException("LPOP command requires at least 1 argument: the key");
        }

        RedisMessage rawKey = context.getArguments().getFirst();
        if (rawKey.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("LPOP command requires the key to be a bulk string");
        }

        String key = (String) rawKey.getContent();

        // check if there's how many to pop
        int popCount = 1;
        if (context.getArguments().size() > 1) {
            RedisMessage rawValue = context.getArguments().get(1);
            if (rawValue.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                throw new IllegalArgumentException("LPOP command requires the pop count to be a bulk string");
            }
            popCount = Integer.parseInt((String) rawValue.getContent());
        }

        // TODO: adapt to transactions
        List<RedisMessage> poppedValues = MemoryManager.popFromList(key, popCount);
        if (poppedValues == null || poppedValues.isEmpty()) {
            context.getOutputStream().write(RedisSerializer.nullBulkString());
        } else if (poppedValues.size() == 1) {
            context.getOutputStream().write(RedisSerializer.serialize(poppedValues.getFirst()));
        } else {
            context.getOutputStream().write(RedisSerializer.list(poppedValues));
        }
    }

    @Override
    public boolean matches(String commandName) {
        return "lpop".equalsIgnoreCase(commandName);
    }
}
