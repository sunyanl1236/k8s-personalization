package lab.personalization.generator.factory;

import lab.personalization.domain.ProductChange;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ProductChangeFactory {
    private static final int OUT_OF_STOCK_IN = 10;

    private final List<String> productIds;
    private final Random random;
    private final Map<String, ProductChange> lastByProduct = new HashMap<>();

    public ProductChangeFactory(List<String> productIds, Random random) {
        this.productIds = productIds;
        this.random = random;
    }

    public ProductChange next(Instant eventTime) {
        String productId = productIds.get(random.nextInt(productIds.size()));
        ProductChange last = lastByProduct.get(productId);
        ProductChange change = last == null
                ? firstSighting(productId, eventTime)
                : moveFrom(last, eventTime);
        lastByProduct.put(productId, change);
        return change;
    }

    private ProductChange firstSighting(String productId, Instant eventTime) {
        double price = nextPrice();
        int stock = nextStock();
        return new ProductChange(productId, eventTime, price, price, stock, stock);
    }

    private ProductChange moveFrom(ProductChange last, Instant eventTime) {
        double previousPrice = last.price();
        int previousStock = last.stock();

        boolean priceMoves = random.nextBoolean();
        boolean stockMoves = random.nextBoolean();
        if (!priceMoves && !stockMoves) {
            priceMoves = true;
        }

        return new ProductChange(
                last.productId(),
                eventTime,
                priceMoves ? nextPrice() : previousPrice,
                previousPrice,
                stockMoves ? nextStock() : previousStock,
                previousStock);
    }

    private double nextPrice() {
        return Math.round((5.0 + random.nextDouble() * 195.0) * 100) / 100.0;
    }

    private int nextStock() {
        return random.nextInt(OUT_OF_STOCK_IN) == 0 ? 0 : 1 + random.nextInt(500);
    }
}
