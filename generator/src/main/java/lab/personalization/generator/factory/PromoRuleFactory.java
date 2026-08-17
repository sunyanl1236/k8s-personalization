package lab.personalization.generator.factory;

import lab.personalization.generator.domain.PromoRule;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class PromoRuleFactory {
    private final AtomicInteger nextId = new AtomicInteger(1);
    private final Random random;

    public PromoRuleFactory(Random random) {
        this.random = random;
    }

    // eventTime unused: PromoRule has no eventTime field, per the domain
    // schema design (broadcast state isn't watermark-gated). Kept as a
    // parameter anyway so all three factories share one Function<Instant,
    // T> shape for SkewedEventStream to call uniformly.
    public PromoRule next(Instant eventTime) {
        String ruleId = "promo-" + nextId.getAndIncrement();
        int discountPercent = 5 + random.nextInt(16); // 5-20%
        String description = discountPercent + "% off, price-drop bonus";
        return new PromoRule(ruleId, description, discountPercent);
    }
}
