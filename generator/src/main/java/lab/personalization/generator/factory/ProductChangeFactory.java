package lab.personalization.generator.factory;

import lab.personalization.generator.domain.PriceChange;
import lab.personalization.generator.domain.ProductChange;
import lab.personalization.generator.domain.StockChange;

import java.time.Instant;
import java.util.List;
import java.util.Random;

public class ProductChangeFactory {
    private final List<String> productIds;
    private final Random random;

    public ProductChangeFactory(List<String> productIds, Random random) {
        this.productIds = productIds;
        this.random = random;
    }

    public ProductChange next(Instant eventTime) {
        String productId = productIds.get(random.nextInt(productIds.size()));
        if (random.nextBoolean()) {
            double newPrice = Math.round((5.0 + random.nextDouble() * 195.0) * 100) / 100.0; // $5.00-$200.00
            return new PriceChange(productId, eventTime, newPrice);
        } else {
            int newStock = random.nextInt(501); // 0-500 units
            return new StockChange(productId, eventTime, newStock);
        }
    }
}
