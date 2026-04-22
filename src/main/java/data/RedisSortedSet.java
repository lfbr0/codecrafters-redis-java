package data;

import serdes.RedisMessage;

import java.util.*;

public class RedisSortedSet extends AbstractSet<RedisSortedSet.RedisSortedSetEntry> {

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

    private Set<String> keySet = new HashSet<>();
    private SortedSet<RedisSortedSetEntry> innerSet = new TreeSet<>(RedisSortedSetEntry::compare);


    public RedisSortedSet copy() {
        RedisSortedSet copy = new RedisSortedSet();
        copy.keySet = new HashSet<>(keySet);
        copy.innerSet = new TreeSet<>(innerSet);
        return copy;
    }

    /**
     * Adds to sorted set
     * @param entry entry to add
     * @return true if added, false if it already existed and just updated
     */
    public boolean add(RedisSortedSetEntry entry) {
        if (keySet.contains(entry.member())) {
            innerSet.removeIf(e -> e.member().equals(entry.member()));
            innerSet.add(entry);
            return false;
        }

        innerSet.add(entry);
        keySet.add(entry.member());
        return true;
    }

    @Override
    public Iterator<RedisSortedSetEntry> iterator() {
        return new TreeSet<>(innerSet).iterator();
    }

    public int size() {
        return keySet.size();
    }

    public Optional<RedisSortedSetEntry> getMember(String member) {
        return new TreeSet<>(innerSet)
                .stream()
                .filter(e -> e.member().equals(member))
                .findFirst();
    }

    public boolean removeMember(String member) {
        return keySet.remove(member) && innerSet.removeIf(e -> e.member().equals(member));
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

    public List<RedisSortedSetEntry> subList(int start, int end) {
        return new TreeSet<>(innerSet).stream()
                .toList()
                .subList(start, end);
    }

}
