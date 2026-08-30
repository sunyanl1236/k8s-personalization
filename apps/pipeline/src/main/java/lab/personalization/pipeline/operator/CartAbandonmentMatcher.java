package lab.personalization.pipeline.operator;

import lab.personalization.domain.Click;
import lab.personalization.domain.ShopperSignal;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class CartAbandonmentMatcher extends PatternProcessFunction<Click, ShopperSignal>
        implements TimedOutPartialMatchHandler<Click> {

    public static final OutputTag<Click> TIMED_OUT =
            new OutputTag<>("cep-timed-out", TypeInformation.of(Click.class)) {};

    // the window expired and no CHECKOUT arrived — a complete match
    @Override
    public void processMatch(Map<String, List<Click>> match, Context ctx, Collector<ShopperSignal> out) {
        Click view = match.get(CartAbandonmentPattern.VIEW).getFirst();
        Click cart = match.get(CartAbandonmentPattern.CART).getFirst();
        out.collect(ShopperSignal.cartAbandonment(view.shopperId(), cart.eventTime(), view.productId()));
    }

    // the window expired with the sequence incomplete — viewed, never carted
    @Override
    public void processTimedOutMatch(Map<String, List<Click>> match, Context ctx) {
        List<Click> views = match.get(CartAbandonmentPattern.VIEW);
        if (views != null && !views.isEmpty()) {
            ctx.output(TIMED_OUT, views.getFirst());
        }
    }
}
