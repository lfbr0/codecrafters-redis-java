package commands.impl.sortedset;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import serdes.RedisMessage;

import java.util.concurrent.Callable;

import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class ZScoreCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
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

            return MemoryManager
                    .getMemberFromSortedSet(key, member)
                    .map(entry -> CommandResponse.bulkString(entry.score().toString()))
                    .orElse(CommandResponse.nullBulkString());
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "ZSCORE".equalsIgnoreCase(commandName);
    }
}
