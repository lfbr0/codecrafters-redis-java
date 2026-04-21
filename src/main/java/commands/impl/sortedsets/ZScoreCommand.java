package commands.impl.sortedsets;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import data.TransactionManager;
import serdes.RedisMessage;

import java.util.concurrent.Callable;

import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class ZScoreCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().size() != 2) {
            throw new IllegalArgumentException("ZSCORE expects exactly two args (key, member)!");
        }

        RedisMessage keyRaw = context.getArguments().getFirst();
        RedisMessage memberRaw = context.getArguments().getLast();
        if (keyRaw.getType() != memberRaw.getType() || memberRaw.getType() != BULK_STRING) {
            throw new IllegalArgumentException("ZSCORE expects its arguments to be bulk strings!");
        }

        String key = keyRaw.getContent().toString();
        String member = memberRaw.getContent().toString();

        Callable<CommandResponse> operation = () -> MemoryManager
                .getMemberFromSortedSet(key, member)
                .map(entry -> CommandResponse.bulkString(entry.score().toString()))
                .orElse(CommandResponse.nullBulkString());

        if (context.isInTransaction()) {
            TransactionManager.addOperation(context.getTransactionId(), () -> operation.call().getResponseBytes());
            return CommandResponse.queued();
        } else {
            return operation.call();
        }
    }

    @Override
    public boolean matches(String commandName) {
        return "ZSCORE".equalsIgnoreCase(commandName);
    }
}
