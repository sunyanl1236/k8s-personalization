package lab.personalization.generator.factory;

import lab.personalization.generator.domain.ActionType;
import lab.personalization.generator.domain.Click;

import java.time.Instant;
import java.util.List;
import java.util.Random;

public class ClickFactory {
    private final List<String> shopperIds;
    private final List<String> productIds;
    private final Random random;

    public ClickFactory(List<String> shopperIds, List<String> productIds, Random random) {
        this.shopperIds = shopperIds;
        this.productIds = productIds;
        this.random = random;
    }

    public Click next(Instant eventTime) {
        String shopperId = shopperIds.get(random.nextInt(shopperIds.size()));
        String productId = productIds.get(random.nextInt(productIds.size()));
        ActionType actionType = ActionType.values()[random.nextInt(ActionType.values().length)];
        return new Click(shopperId, productId, eventTime, actionType);
    }
}
