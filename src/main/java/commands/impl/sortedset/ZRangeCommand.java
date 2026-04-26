package commands.impl.sortedset;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.List;
import java.util.concurrent.Callable;

public class ZRangeCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() < 3) {
                throw new IllegalArgumentException("ZRANGE command requires at least 3 arguments: key, start, stop");
            }

            RedisMessage keyRaw = context.getArguments().get(0);
            RedisMessage startRaw = context.getArguments().get(1);
            RedisMessage stopRaw = context.getArguments().get(2);

            if (keyRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING ||
                    startRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING ||
                    stopRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                throw new IllegalArgumentException("ZRANGE command arguments must be bulk strings");
            }

            String key = (String) keyRaw.getContent();
            int start = Integer.parseInt((String) startRaw.getContent());
            int stop = Integer.parseInt((String) stopRaw.getContent());

            List<String> range = MemoryManager.rangeFromSortedList(key, start, stop);
            return new CommandResponse(RedisSerializer.listStrings(range));
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "ZRANGE".equalsIgnoreCase(commandName);
    }
}
