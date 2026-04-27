package commands.impl.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class StreamEntry implements Comparable<StreamEntry> {

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

    @Override
    public int compareTo(StreamEntry streamEntry) {
        String[] myEntryParts = streamEntryId.split("-");
        long myTs = Long.parseLong(myEntryParts[0]);
        long mySeq = Long.parseLong(myEntryParts[1]);

        String[] entryParts = streamEntry.getStreamEntryId().split("-");
        long entryTs = Long.parseLong(entryParts[0]);
        long entrySeq = Long.parseLong(entryParts[1]);

        long deltaTs = myTs - entryTs;
        if (deltaTs != 0)
            return Long.valueOf(deltaTs).intValue();

        return Long.valueOf(mySeq - entrySeq).intValue();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof StreamEntry that)) return false;
        return streamKey.equals(that.streamKey) && streamEntryId.equals(that.streamEntryId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamKey, streamEntryId);
    }
}
