package lab.personalization.generator.factory;

import java.util.List;

// Shared identifier pools, so a Click and a ProductChange can reference the
// same product, giving Phase 4's interval join something to actually match
// against instead of two disjoint universes of IDs.
public final class Catalog {
    public static final List<String> SHOPPER_IDS = List.of(
            "shopper-1", "shopper-2", "shopper-3", "shopper-4", "shopper-5",
            "shopper-6", "shopper-7", "shopper-8", "shopper-9", "shopper-10");

    public static final List<String> PRODUCT_IDS = List.of(
            "P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10");

    private Catalog() {}
}
