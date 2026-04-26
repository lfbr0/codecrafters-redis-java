package commands.impl.transaction;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.TransactionManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.concurrent.Callable;

public class WatchCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (context.isInTransaction()) {
            return CommandResponse.error("ERR WATCH inside MULTI is not allowed");
        }
        return handleContext(context).call();
    }

    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().isEmpty()) {
                throw new IllegalArgumentException("WATCH expects at least 1 argument!");
            }

            for (RedisMessage keyRaw : context.getArguments()) {
                if (keyRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                    throw new IllegalArgumentException("WATCH expects arguments to be BULK STRING!");
                }

                String key = keyRaw.getContent().toString();
                TransactionManager.watchKey(key, context.getClientUUID());
            }

            return new CommandResponse(RedisSerializer.okString());
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "WATCH".equalsIgnoreCase(commandName);
    }
}
