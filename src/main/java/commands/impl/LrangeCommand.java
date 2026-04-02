package commands.impl;

import commands.Command;
import commands.CommandContext;
import data.MemoryManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.List;

public class LrangeCommand implements Command {
    @Override
    public void execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().size() < 3) {
            throw new IllegalArgumentException("LRANGE command requires at least 3 arguments: key, start, stop");
        }

        RedisMessage keyRaw = context.getArguments().get(0);
        RedisMessage startRaw = context.getArguments().get(1);
        RedisMessage stopRaw = context.getArguments().get(2);

        if (keyRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING ||
                startRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING ||
                stopRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("LRANGE command arguments must be bulk strings");
        }

        String key = (String) keyRaw.getContent();
        int start = Integer.parseInt((String) startRaw.getContent());
        int stop = Integer.parseInt((String) stopRaw.getContent());

        // TODO: prepare for transactions
        List<RedisMessage> range = MemoryManager.rangeFromList(key, start, stop);
        context.getOutputStream().write(RedisSerializer.list(range));
    }

    @Override
    public boolean matches(String commandName) {
        return "lrange".equalsIgnoreCase(commandName);
    }
}
