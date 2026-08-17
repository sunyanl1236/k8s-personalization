package lab.personalization.generator.domain;

import java.time.Instant;

public record Recommendation(String shopperId, String productId, double discountPercent, String reason, Instant generatedAt) {}
