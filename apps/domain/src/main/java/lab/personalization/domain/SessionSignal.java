package lab.personalization.domain;

import java.time.Instant;

public record SessionSignal(
        String shopperId,
        Instant windowStart,
        Instant windowEnd,
        int clickCount,
        String topProductId) {}
