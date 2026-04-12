package data;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class RedisSortedSet {

    public record RedisSortedSetEntry(String member, Double score) {
        public static int compare(RedisSortedSetEntry e1, RedisSortedSetEntry e2) {
            return Double.compare(e1.score(), e2.score());
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

}
