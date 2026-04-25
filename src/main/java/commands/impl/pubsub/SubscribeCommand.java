package commands.impl.pubsub;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.TransactionManager;
import pubsub.PubSubManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.List;
import java.util.concurrent.Callable;

import static serdes.RedisMessage.RedisMessageType.*;

public class SubscribeCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().size() != 1) {
            throw new IllegalArgumentException("SUBSCRIBE expects exactly 1 argument!");
        }

        RedisMessage channelRaw = context.getArguments().getFirst();
        if (channelRaw.getType() != BULK_STRING) {
            throw new IllegalArgumentException("SUBSCRIBE expects channel argument to be BULK STRING!");
        }

        final String channel = channelRaw.getContent().toString();
        Callable<CommandResponse> operation = () -> {
            int subscribedChannelsByClient = PubSubManager.getInstance()
                    .registerSubscription(context.getClientUUID(), context.getOutputStream(), channel);

            RedisMessage msg1 = new RedisMessage().setType(BULK_STRING).setContent("subscribe");
            RedisMessage msg2 = new RedisMessage().setType(BULK_STRING).setContent(channel);
            RedisMessage msg3 = new RedisMessage().setType(INTEGER).setContent(subscribedChannelsByClient);

            RedisMessage subMsg = new RedisMessage().setType(ARRAY).setContent(List.of(msg1, msg2, msg3));
            return new CommandResponse(RedisSerializer.serialize(subMsg));
        };

        if (context.isInTransaction()) {
            TransactionManager.addOperation(context.getTransactionId(), () -> operation.call().getResponseBytes());
            return CommandResponse.queued();
        }

        return operation.call();
    }

    @Override
    public boolean matches(String commandName) {
        return "SUBSCRIBE".equalsIgnoreCase(commandName);
    }
}
