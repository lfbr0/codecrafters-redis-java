package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.TransactionManager;
import pubsub.PubSubManager;
import serdes.RedisSerializer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

public class PingCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        Callable<byte[]> task = () -> {
            if (PubSubManager.getInstance().getClientSubscriptions(context.getClientUUID()) > 0) {
                return RedisSerializer.listStrings(List.of("pong", ""));
            } else {
                return RedisSerializer.simpleString("PONG");
            }
        };

        if (context.isInTransaction()) {
            TransactionManager.addOperation(context.getTransactionId(), task);
            return CommandResponse.queued();
        }

        return new CommandResponse(task.call());
    }

    @Override
    public boolean matches(String commandName) {
        return "ping".equalsIgnoreCase(commandName);
    }

    @Override
    public boolean isSubscriberModeAllowedCommand() {
        return true;
    }
}
