package lab.personalization.domain;

import java.time.Instant;

public record EnrichedClick(
        String shopperId,
        String productId,
        Instant clickTime,
        double price,
        double previousPrice,
        int stock,
        Instant changeTime) {

    public boolean priceDropped() {
        return price < previousPrice;
    }

    public boolean outOfStock() {
        return stock == 0;
    }
}
