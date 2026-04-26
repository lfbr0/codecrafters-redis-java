package commands.impl.sortedset;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import serdes.RedisMessage;

import java.util.concurrent.Callable;

public class ZCardCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() != 1) {
                throw new IllegalArgumentException("ZCARD command accepts exactly one argument!");
            }

            RedisMessage keyRaw = context.getArguments().getFirst();
            if (keyRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                throw new IllegalArgumentException("ZCARD command argument must be bulk string of sorted set key!");
            }

            String key = keyRaw.getContent().toString();
            return CommandResponse.integer(MemoryManager.lengthOfSortedSet(key));
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "ZCARD".equalsIgnoreCase(commandName);
    }
}
