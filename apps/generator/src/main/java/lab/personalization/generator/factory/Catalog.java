package lab.personalization.generator.factory;

import java.util.List;
import java.util.stream.IntStream;

// Shared identifier pools, so a Click and a ProductChange can reference the
// same product, giving Phase 4's interval join something to actually match
// against instead of two disjoint universes of IDs.
//
// Sized from the CLI since Phase 6: ten keys cannot fill six subtasks, and
// session length depends on how many Shoppers the Click rate is spread over.
public final class Catalog {
    public static List<String> shopperIds(int count) {
        return ids("shopper-", count);
    }

    public static List<String> productIds(int count) {
        return ids("P", count);
    }

    private static List<String> ids(String prefix, int count) {
        return IntStream.rangeClosed(1, count)
                .mapToObj(i -> prefix + i)
                .toList();
    }

    private Catalog() {}
}
