package lab.personalization.domain;

import java.time.Instant;

// A Signal by CONTEXT.md's definition: the output of one analytical branch
// about one Shopper, before any decision is made. It lives in :domain rather
// than :pipeline because it is domain vocabulary, and Phase 4 adds more
// Signals alongside it. Like everything else here it has no dependencies, so
// it does not break the module's one rule.
//
// public, unlike PipelineConfig, for two reasons. :pipeline is a different
// module and has to see it. And Flink recognises a record as a POJO type only
// if it is public; a package-private one falls back to a generic type, which
// pipeline.generic-types: false then rejects at graph construction.
//
// windowStart and windowEnd come from the window, never from wall-clock time.
// Task 6 uses windowEnd as the Recommendation's generatedAt, and Task 9's
// Drill compares two runs line for line; a wall-clock stamp would differ on
// every line between them and make the comparison meaningless.
public record SessionSignal(
        String shopperId,
        Instant windowStart,
        Instant windowEnd,
        int clickCount,
        String topProductId) {}
