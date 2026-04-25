package pubsub;

import logger.Logger;

import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.emptySet;
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

    public Set<String> getClientSubscriptions(UUID clientUUID) {
        return new HashSet<>(clientToChannelsMap.getOrDefault(clientUUID, Set.of()));
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

}
