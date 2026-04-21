package commands.impl.list;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import data.TransactionManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.concurrent.Callable;

public class LpushCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().size() < 2) {
            throw new IllegalArgumentException("LPUSH command requires at least 2 arguments: key and value(s)");
        }

        Callable<byte[]> task = () -> {
            RedisMessage[] args = new RedisMessage[context.getArguments().size() - 1];
            for (int i = 1; i < context.getArguments().size(); i++) {
                args[i - 1] = context.getArguments().get(i);
            }
            String key = (String) context.getArguments().getFirst().getContent();
            int newSize = MemoryManager.prependToList(key, args);
            return RedisSerializer.integer(newSize);
        };

        if (context.isInTransaction()) {
            TransactionManager.addOperation(context.getTransactionId(), task);
            return CommandResponse.queued();
        }

        return new CommandResponse(task.call());
    }

    @Override
    public boolean matches(String commandName) {
        return "lpush".equalsIgnoreCase(commandName);
    }
}
