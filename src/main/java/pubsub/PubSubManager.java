package pubsub;

import logger.Logger;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.newSetFromMap;

/**
 * Manages Redis pub/sub functionality for multiple clients and channels.
 * Implements the singleton pattern and uses thread-safe data structures.
 */
public class PubSubManager {

    private static PubSubManager INSTANCE;
    // non-static fields
    private final Map<UUID, Set<String>> clientToChannelsMap = new ConcurrentHashMap<>();
    private final Map<String, Set<RedisSubscription>> channelToClientsMap = new ConcurrentHashMap<>();

    /**
     * Represents a client subscription to a channel.
     */
    public record RedisSubscription(UUID clientUUID, String channel, OutputStream outputStream){
    }

    public static synchronized PubSubManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new PubSubManager();
        }
        return INSTANCE;
    }

    /**
     * Gets the number of channels a client is subscribed to.
     *
     * @param clientUUID the client's unique identifier
     * @return the number of active subscriptions for the client
     */
    public int getClientSubscriptions(UUID clientUUID) {
        return clientToChannelsMap.getOrDefault(clientUUID, Set.of()).size();
    }

    /**
     * Subscribes a client to a channel.
     *
     * @param clientUUID the client's unique identifier
     * @param outputStream the client's output stream for receiving messages
     * @param channel the channel name
     * @return the total number of subscriptions for the client after subscribing
     */
    public int subscribe(UUID clientUUID, OutputStream outputStream, String channel) {
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

    /**
     * Unsubscribes a client from a channel.
     *
     * @param clientUUID the client's unique identifier
     * @param channel the channel name
     * @return the total number of subscriptions for the client after unsubscribing
     */
    public int unsubscribe(UUID clientUUID, String channel) {
        Logger.info("PubSub - unsubscribing client " + clientUUID + " from channel " + channel);

        // remove sub from client map
        Set<String> subbedChannels = clientToChannelsMap.compute(clientUUID, (key, existingSet) -> {
            Set<String> set = (existingSet == null) ? newSetFromMap(new ConcurrentHashMap<>()) : existingSet;
            set.remove(channel);
            return set;
        });

        // add sub to subs map
        channelToClientsMap.compute(channel, (key, existingSet) -> {
            Set<RedisSubscription> set = (existingSet == null) ? newSetFromMap(new ConcurrentHashMap<>()) : existingSet;
            set.removeIf(sub -> sub.clientUUID().equals(clientUUID));
            return set;
        });

        return subbedChannels.size();
    }

    /**
     * Publishes a message to all clients subscribed to a channel.
     * Serializes the message in Redis protocol format and sends it to each subscriber's output stream.
     *
     * @param channel the channel name
     * @param message the message content
     * @return the number of clients that successfully received the message
     */
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
                Logger.error("Failed to publish message to client " + sub.clientUUID() + ": " + e.getMessage());
            }
        }
        return count;
    }

}
