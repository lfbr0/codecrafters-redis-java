package pubsub;

import logger.Logger;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.newSetFromMap;

public class PubSubManager {

    private static PubSubManager INSTANCE;
    // non-static fields
    private final Map<UUID, Set<String>> clientToChannelsMap = new ConcurrentHashMap<>();
    private final Map<String, Set<RedisSubscription>> channelToClientsMap = new ConcurrentHashMap<>();

    /**
     * Class to store redis sub
     */
    public record RedisSubscription(UUID uuid, String channel, OutputStream outputStream){
    }

    public static synchronized PubSubManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new PubSubManager();
        }
        return INSTANCE;
    }

    public int getClientSubscriptions(UUID clientUUID) {
        return clientToChannelsMap.getOrDefault(clientUUID, Set.of()).size();
    }

    public int registerSubscription(UUID clientUUID, OutputStream outputStream, String channel) {
        Logger.info("PubSub - registering client " + clientUUID + " subscription to channel " + channel);

        // add sub to client map
        Set<String> subbedChannels = clientToChannelsMap.compute(clientUUID, (key, existingSet) -> {
            Set<String> set = (existingSet == null) ? newSetFromMap(new ConcurrentHashMap<>()) : existingSet;
            set.add(channel);
            return set;
        });

        // add sub to subs map
        channelToClientsMap.compute(channel, (key, existingSet) -> {
            Set<RedisSubscription> set = (existingSet == null) ? newSetFromMap(new ConcurrentHashMap<>()) : existingSet;
            set.add(new RedisSubscription(clientUUID, channel, outputStream));
            return set;
        });

        return subbedChannels.size();
    }

    public int publish(String channel, String message) {
        Set<RedisSubscription> subscriptions = channelToClientsMap.getOrDefault(channel, Set.of());
        if (subscriptions.isEmpty()) {
            return 0;
        }

        RedisMessage msg1 = new RedisMessage().setType(RedisMessage.RedisMessageType.BULK_STRING).setContent("message");
        RedisMessage msg2 = new RedisMessage().setType(RedisMessage.RedisMessageType.BULK_STRING).setContent(channel);
        RedisMessage msg3 = new RedisMessage().setType(RedisMessage.RedisMessageType.BULK_STRING).setContent(message);
        RedisMessage pubMsg = new RedisMessage().setType(RedisMessage.RedisMessageType.ARRAY).setContent(List.of(msg1, msg2, msg3));

        byte[] serializedMsg = RedisSerializer.serialize(pubMsg);

        int count = 0;
        for (RedisSubscription sub : subscriptions) {
            try {
                sub.outputStream().write(serializedMsg);
                sub.outputStream().flush();
                count++;
            } catch (Exception e) {
                Logger.error("Failed to publish message to client " + sub.uuid() + ": " + e.getMessage());
            }
        }
        return count;
    }

}
