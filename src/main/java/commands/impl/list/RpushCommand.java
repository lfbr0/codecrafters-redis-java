package commands.impl.list;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import data.TransactionManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.concurrent.Callable;

public class RpushCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() < 2) {
                throw new IllegalArgumentException("RPUSH command requires at least 2 arguments: key and value(s)");
            }

            RedisMessage[] args = new RedisMessage[context.getArguments().size() - 1];
            for (int i = 1; i < context.getArguments().size(); i++) {
                args[i - 1] = context.getArguments().get(i);
            }
            String key = (String) context.getArguments().getFirst().getContent();
            int newSize = MemoryManager.appendToList(key, args);
            return new CommandResponse(RedisSerializer.integer(newSize));
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "rpush".equalsIgnoreCase(commandName);
    }
}
