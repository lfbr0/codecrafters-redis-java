package commands.impl.stream;

import java.util.AbstractList;
import java.util.ArrayList;

public class RedisStream extends AbstractList<StreamEntry> {

    private final ArrayList<StreamEntry> internalList = new ArrayList<>();

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
}
