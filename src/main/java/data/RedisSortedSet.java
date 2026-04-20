package data;

import java.util.*;

public class RedisSortedSet {

    public record RedisSortedSetEntry(String member, Double score) {
        public static int compare(RedisSortedSetEntry e1, RedisSortedSetEntry e2) {
            int cmp = Comparator
                    .comparingDouble(RedisSortedSetEntry::score)
                    .compare(e1, e2);

            if (cmp == 0) {
                return e1.member().compareTo(e2.member());
            }

            return cmp;
        }
    }

    private final Set<String> keySet = new HashSet<>();
    private final SortedSet<RedisSortedSetEntry> innerSet = new TreeSet<>(RedisSortedSetEntry::compare);

    public boolean add(RedisSortedSetEntry entry) {
        if (keySet.contains(entry.member())) {
            return false;
        }
        innerSet.add(entry);
        keySet.add(entry.member());
        return true;
    }

    public Optional<Integer> indexOf(String member) {
        if (!keySet.contains(member)) {
            return Optional.empty();
        }

        Iterator<RedisSortedSetEntry> it = new TreeSet<>(innerSet).iterator();
        int index = 0;
        while (it.hasNext()) {
            if (it.next().member().equals(member)) {
                break;
            }
            index++;
        }
        return Optional.of(index);
    }

}
