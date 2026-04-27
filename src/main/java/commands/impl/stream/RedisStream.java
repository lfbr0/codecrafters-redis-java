package commands.impl.stream;

import java.util.*;

public class RedisStream extends AbstractList<StreamEntry> {

    private final ArrayList<StreamEntry> internalList = new ArrayList<>();

    public Optional<String> addWithAutoGeneration(StreamEntry newEntry) {
        String newEntryId = newEntry.getStreamEntryId();
        // there's a fully defined id, we can use it to the default method
        if (newEntryId.matches("[0-9]+-[0-9]+")) {
            return add(newEntry) ? Optional.of(newEntryId) : Optional.empty();
        }
        // must generate sequence id
        if (newEntryId.matches("[0-9]+-\\*")) {
            String newEntryTs = newEntryId.split("-")[0];
            long seqId = findNextAvailableSequenceIdByTimestamp(newEntryTs);

            newEntryId = newEntryId.replaceAll("\\*", "" + seqId);

            // bug fix - cannot use 0-0
            if (newEntryId.equals("0-0")) {
                newEntryId = "0-1";
            }

            StreamEntry updatedNewEntry = new StreamEntry(newEntry.getStreamKey(), newEntryId);
            return add(updatedNewEntry) ? Optional.of(newEntryId) : Optional.empty();
        }
        // must generate everything
        if (newEntryId.matches("\\*")) {
            String entryTs = Long.toString(System.currentTimeMillis());
            long seqId = findNextAvailableSequenceIdByTimestamp(entryTs);
            newEntryId = entryTs + "-" + seqId;

            StreamEntry updatedNewEntry = new StreamEntry(newEntry.getStreamKey(), newEntryId);
            return add(updatedNewEntry) ? Optional.of(newEntryId) : Optional.empty();
        }

        // no case for this situation
        return Optional.empty();
    }

    private long findNextAvailableSequenceIdByTimestamp(String newEntryTs) {
        OptionalLong maxSeq = internalList.stream()
                .filter(streamEntry -> streamEntry.getStreamEntryId().startsWith(newEntryTs + "-"))
                .map(streamEntry -> streamEntry.getStreamEntryId().split("-")[1])
                .mapToLong(Long::parseLong)
                .max();

        if (maxSeq.isPresent()) {
            return maxSeq.getAsLong() + 1;
        }
        return 0;
    }

    /**
     * If empty add as last. If not, check if last entry id is bigger than all
     * @param newEntry entry to add
     * @return true if valid entry (youngest)
     */
    @Override
    public boolean add(StreamEntry newEntry) {
        if (internalList.isEmpty()) {
            return internalList.add(newEntry);
        }
        StreamEntry lastEntry = internalList.getLast();
        // new entry must be younger than last entry (bigger)
        return newEntry.compareTo(lastEntry) > 0 && internalList.add(newEntry);
    }

    @Override
    public StreamEntry get(int i) {
        return internalList.get(i);
    }

    @Override
    public int size() {
        return internalList.size();
    }

    /**
     * Returns range from Redis Stream with corresponding entries
     *
     * @param startEntryId starting entry id
     * @param endEntryId ending entry id (can be null for no end)
     * @param inclusive whether the startEntryId is inclusive
     * @return list of matching entry ids
     */
    public List<StreamEntry> range(String startEntryId, String endEntryId, boolean inclusive) {
        return internalList.stream()
                .filter(streamEntry -> {
                    String streamKey = streamEntry.getStreamKey();

                    // do not accept if stream entry is older than start entry
                    int comparison = streamEntry.compareTo(new StreamEntry(streamKey, startEntryId));
                    if (inclusive) {
                        if (comparison < 0) return false;
                    } else {
                        if (comparison <= 0) return false;
                    }

                    // if end entry is specified, stream entry has to be older than it
                    if (endEntryId != null) {
                        return streamEntry.compareTo(new StreamEntry(streamKey, endEntryId)) <= 0;
                    }

                    // passed all filters
                    return true;
                })
                .toList();
    }

    /**
     * Returns range from Redis Stream with corresponding entries (inclusive)
     *
     * @param startEntryId starting entry id
     * @param endEntryId ending entry id
     * @return list of matching entry ids
     */
    public List<StreamEntry> range(String startEntryId, String endEntryId) {
        return range(startEntryId, endEntryId, true);
    }
}
