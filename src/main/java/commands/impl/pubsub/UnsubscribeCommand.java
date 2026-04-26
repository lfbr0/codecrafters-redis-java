package commands.impl.pubsub;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import pubsub.PubSubManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.List;
import java.util.concurrent.Callable;

import static serdes.RedisMessage.RedisMessageType.*;

public class UnsubscribeCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() != 1) {
                throw new IllegalArgumentException("SUBSCRIBE expects exactly 1 argument!");
            }

            RedisMessage channelRaw = context.getArguments().getFirst();
            if (channelRaw.getType() != BULK_STRING) {
                throw new IllegalArgumentException("SUBSCRIBE expects channel argument to be BULK STRING!");
            }

            final String channel = channelRaw.getContent().toString();

            int subscribedChannelsByClient = PubSubManager.getInstance()
                    .unsubscribe(context.getClientUUID(), channel);

            RedisMessage msg1 = new RedisMessage().setType(BULK_STRING).setContent("unsubscribe");
            RedisMessage msg2 = new RedisMessage().setType(BULK_STRING).setContent(channel);
            RedisMessage msg3 = new RedisMessage().setType(INTEGER).setContent(subscribedChannelsByClient);

            RedisMessage subMsg = new RedisMessage().setType(ARRAY).setContent(List.of(msg1, msg2, msg3));
            return new CommandResponse(RedisSerializer.serialize(subMsg));
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "UNSUBSCRIBE".equalsIgnoreCase(commandName);
    }

    @Override
    public boolean isSubscriberModeAllowedCommand() {
        return true;
    }
}
