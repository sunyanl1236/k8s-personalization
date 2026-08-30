package lab.personalization.pipeline.operator;

import lab.personalization.domain.ActionType;
import lab.personalization.domain.Click;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.time.Duration;

public final class CartAbandonmentPattern {

    static final String VIEW = "view";
    static final String CART = "cart";
    static final String CHECKOUT = "checkout";

    public static Pattern<Click, ?> pattern(Duration within) {
        return Pattern.<Click>begin(VIEW) // step names
                // SimpleCondition: the candidate event only
                // "is this an ADD_TO_CART?"
                .where(SimpleCondition.of(click -> click.actionType() == ActionType.VIEW))
                .followedBy(CART) // step names
                .where(new SameProductAs(VIEW, ActionType.ADD_TO_CART))
                .notFollowedBy(CHECKOUT) // step names
                .where(new SameProductAs(VIEW, ActionType.CHECKOUT))
                .within(within);
    }

    // IterativeCondition: the candidate plus the match so far
    // "is this an ADD_TO_CART on the same Product as the view?"
    private static final class SameProductAs extends IterativeCondition<Click> {
        private final String earlierStep;
        private final ActionType action;

        private SameProductAs(String earlierStep, ActionType action) {
            this.earlierStep = earlierStep;
            this.action = action;
        }

        @Override
        public boolean filter(Click candidate, Context<Click> ctx) throws Exception {
            if (candidate.actionType() != action) {
                return false;
            }
            for (Click earlier : ctx.getEventsForPattern(earlierStep)) {
                if (earlier.productId().equals(candidate.productId())) {
                    return true;
                }
            }
            return false;
        }
    }

    private CartAbandonmentPattern() {}
}
