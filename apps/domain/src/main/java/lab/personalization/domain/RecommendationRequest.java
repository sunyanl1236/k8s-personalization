package lab.personalization.domain;

import java.time.Instant;

public record RecommendationRequest(
        String shopperId,
        String candidateProductId,
        boolean priceDropMatched,
        boolean cartAbandoned,
        double discountPercent,
        String reason,
        Instant generatedAt) {

    public RecommendationRequest withDiscount(double newDiscountPercent) {
        return new RecommendationRequest(shopperId, candidateProductId, priceDropMatched,
                cartAbandoned, newDiscountPercent, reason, generatedAt);
    }
}
