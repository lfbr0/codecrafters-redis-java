package data;

public class SortedSet {

    public record SortedSetEntry(String member, Double Score) {
    }

    public boolean add(SortedSetEntry entry) {
        return true;
    }

}
