package commands.impl.sortedsets;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import data.TransactionManager;
import serdes.RedisMessage;

import java.util.concurrent.Callable;

public class ZCardCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().size() != 1) {
            throw new IllegalArgumentException("ZCARD command accepts exactly one argument!");
        }

        RedisMessage keyRaw = context.getArguments().getFirst();
        if (keyRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("ZCARD command argument must be bulk string of sorted set key!");
        }

        String key = keyRaw.getContent().toString();
        Callable<byte[]> operation = () -> CommandResponse
                .integer(MemoryManager.lengthOfSortedSet(key))
                .getResponseBytes();

        if (context.isInTransaction()) {
            TransactionManager.addOperation(context.getTransactionId(), operation);
            return CommandResponse.queued();
        } else {
            return new CommandResponse(operation.call());
        }
    }

    @Override
    public boolean matches(String commandName) {
        return "ZCARD".equalsIgnoreCase(commandName);
    }
}
