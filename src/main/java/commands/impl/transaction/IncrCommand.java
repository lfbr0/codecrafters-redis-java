package commands.impl.transaction;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import data.TransactionManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.concurrent.Callable;

import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class IncrCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().size() != 1) {
            throw new IllegalArgumentException("INCR command requires exactly 1 argument");
        }

        RedisMessage keyRaw = context.getArguments().getFirst();
        if (keyRaw.getType() != BULK_STRING) {
            throw new IllegalArgumentException("INCR command requires a bulk string argument");
        }

        String key = (String) keyRaw.getContent();

        Callable<byte[]> operation = () -> {
            try {
                int result = MemoryManager.increment(key);
                return RedisSerializer.integer(result);
            } catch (IllegalArgumentException e) {
                return RedisSerializer.error(e.getMessage());
            }
        };

        if (context.isInTransaction()) {
            TransactionManager.addOperation(context.getTransactionId(), operation);
            return CommandResponse.queued();
        } else {
            return new CommandResponse(operation.call());
        }
    }


    @Override
    public boolean matches(String commandName) {
        return "incr".equalsIgnoreCase(commandName);
    }
}
