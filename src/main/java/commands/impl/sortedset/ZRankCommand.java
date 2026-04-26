package commands.impl.sortedset;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.concurrent.Callable;

import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class ZRankCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
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

            return MemoryManager
                    .rankFromSortedSet(zSetKey, zSetMember)
                    .map(rank -> new CommandResponse(RedisSerializer.integer(rank)))
                    .orElse(new CommandResponse(RedisSerializer.nullBulkString()));
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "ZRANK".equalsIgnoreCase(commandName);
    }
}
