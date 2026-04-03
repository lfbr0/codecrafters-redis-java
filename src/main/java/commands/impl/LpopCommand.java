package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import data.TransactionManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class LpopCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().isEmpty()) {
            throw new IllegalArgumentException("LPOP command requires at least 1 argument: the key");
        }

        RedisMessage rawKey = context.getArguments().getFirst();
        if (rawKey.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("LPOP command requires the key to be a bulk string");
        }

        String key = (String) rawKey.getContent();

        // check if there's how many to pop
        final AtomicInteger popCount = new AtomicInteger(1);
        if (context.getArguments().size() > 1) {
            RedisMessage rawValue = context.getArguments().get(1);
            if (rawValue.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                throw new IllegalArgumentException("LPOP command requires the pop count to be a bulk string");
            }
            popCount.set(Integer.parseInt((String) rawValue.getContent()));
        }

        Callable<byte[]> task = () -> {
            List<RedisMessage> poppedValues = MemoryManager.popFromList(key, popCount.get());
            if (poppedValues.isEmpty()) {
                return RedisSerializer.nullBulkString();
            } else if (poppedValues.size() == 1) {
                return RedisSerializer.serialize(poppedValues.getFirst());
            } else {
                return RedisSerializer.list(poppedValues);
            }
        };

        if (context.isInTransaction()) {
            TransactionManager.addOperation(context.getTransactionId(), task);
            return CommandResponse.queued();
        } else {
            return new CommandResponse(task.call());
        }
    }

    @Override
    public boolean matches(String commandName) {
        return "lpop".equalsIgnoreCase(commandName);
    }
}
