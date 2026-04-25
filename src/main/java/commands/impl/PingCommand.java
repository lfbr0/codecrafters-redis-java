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
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (PubSubManager.getInstance().getClientSubscriptions(context.getClientUUID()) > 0) {
                return new CommandResponse(RedisSerializer.listStrings(List.of("pong", "")));
            } else {
                return new CommandResponse(RedisSerializer.simpleString("PONG"));
            }
        };
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
