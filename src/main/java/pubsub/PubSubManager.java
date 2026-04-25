package pubsub;

import java.io.OutputStream;
import java.util.UUID;

public class PubSubManager {

    private static PubSubManager INSTANCE;

    public static synchronized PubSubManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new PubSubManager();
        }
        return INSTANCE;
    }

    public void registerSubscription(UUID clientUUID, OutputStream outputStream, String channel) {

    }

}
