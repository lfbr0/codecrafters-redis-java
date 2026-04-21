package commands.impl.sortedsets;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import data.TransactionManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.concurrent.Callable;

import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class ZRankCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().size() != 2) {
            throw new IllegalArgumentException("ZRANK expects exactly 2 arguments!");
        }

        RedisMessage zSetKeyRaw = context.getArguments().getFirst();
        RedisMessage zSetMemberRaw = context.getArguments().getLast();
        if (zSetMemberRaw.getType() != zSetKeyRaw.getType() || zSetMemberRaw.getType() != BULK_STRING) {
            throw new IllegalArgumentException("ZRANK set key and member should be BULK STRING!");
        }

        String zSetKey = zSetKeyRaw.getContent().toString();
        String zSetMember = zSetMemberRaw.getContent().toString();

        Callable<byte[]> operation = () -> MemoryManager
                .rankFromSortedSet(zSetKey, zSetMember)
                .map(RedisSerializer::integer)
                .orElse(RedisSerializer.nullBulkString());

        if (context.isInTransaction()) {
            TransactionManager.addOperation(context.getTransactionId(), operation);
            return CommandResponse.queued();
        } else {
            return new CommandResponse(operation.call());
        }
    }

    @Override
    public boolean matches(String commandName) {
        return "ZRANK".equalsIgnoreCase(commandName);
    }
}
