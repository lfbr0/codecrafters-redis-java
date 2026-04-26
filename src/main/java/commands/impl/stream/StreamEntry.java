package commands.impl.stream;

import java.util.HashMap;
import java.util.Map;

public class StreamEntry {

    private final String streamKey;
    private final String streamEntryId;
    private final HashMap<String, String> properties;

    public StreamEntry(String streamKey, String streamEntryId) {
        this.streamKey = streamKey;
        this.streamEntryId = streamEntryId;
        this.properties = new HashMap<String, String>();
    }

    public StreamEntry addProperty(String propKey, String propValue) {
        properties.put(propKey, propValue);
        return this;
    }

    public String getStreamKey() {
        return streamKey;
    }

    public String getStreamEntryId() {
        return streamEntryId;
    }

    public HashMap<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "StreamEntry{" +
                "streamKey='" + streamKey + '\'' +
                ", streamEntryId='" + streamEntryId + '\'' +
                ", properties=" + properties +
                '}';
    }
}
