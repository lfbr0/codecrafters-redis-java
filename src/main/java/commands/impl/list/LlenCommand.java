package commands.impl.list;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import data.TransactionManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.concurrent.Callable;

public class LlenCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().isEmpty()) {
                throw new IllegalArgumentException("LLEN command requires a key argument");
            }

            RedisMessage rawKey = context.getArguments().getFirst();
            if (rawKey == null || rawKey.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                throw new IllegalArgumentException("LLEN command requires a bulk string key argument");
            }

            String key = (String) rawKey.getContent();

            return new CommandResponse(RedisSerializer.integer(MemoryManager.lengthOfList(key)));
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "llen".equalsIgnoreCase(commandName);
    }
}
