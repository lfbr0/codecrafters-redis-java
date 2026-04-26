package commands.impl.transaction;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import serdes.RedisMessage;

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
            if (context.getArguments() == null || context.getArguments().size() != 1) {
                throw new IllegalArgumentException("WATCH expects exactly 1 argument!");
            }

            RedisMessage keyRaw = context.getArguments().getFirst();
            if (keyRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                throw new IllegalArgumentException("WATCH expects argument to be BULK STRING!");
            }

            String key = keyRaw.getContent().toString();
            return CommandResponse.ok();
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "WATCH".equalsIgnoreCase(commandName);
    }
}
