package commands.impl.pubsub;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import pubsub.PubSubManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.concurrent.Callable;

import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class PublishCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() != 2) {
                throw new IllegalArgumentException("PUBLISH expects exactly 2 arguments!");
            }

            RedisMessage channelRaw = context.getArguments().get(0);
            RedisMessage messageRaw = context.getArguments().get(1);

            if (channelRaw.getType() != BULK_STRING || messageRaw.getType() != BULK_STRING) {
                throw new IllegalArgumentException("PUBLISH expects channel and message arguments to be BULK STRING!");
            }

            String channel = channelRaw.getContent().toString();
            String message = messageRaw.getContent().toString();

            int receivers = PubSubManager.getInstance().publish(channel, message);

            return new CommandResponse(RedisSerializer.integer(receivers));
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "PUBLISH".equalsIgnoreCase(commandName);
    }
}
