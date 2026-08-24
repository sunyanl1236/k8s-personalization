package lab.personalization.pipeline;

import java.time.Duration;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import lab.personalization.domain.Recommendation;
import lab.personalization.domain.SessionSignal;

public class RecommendationDecider
    extends KeyedProcessFunction<String, SessionSignal, Recommendation> {

    private static final String REASON = "most-viewed-in-session";

    private final Duration cooldown;

    private transient ValueState<String> lastRecommendedProduct;
    private transient ValueState<Long> pendingTimer;

    public RecommendationDecider(Duration cooldown) {
        this.cooldown = cooldown;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        lastRecommendedProduct = getRuntimeContext().getState(new ValueStateDescriptor<>("last-recommended-product", String.class));
        pendingTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("pending-timer", Long.class));
    }

    @Override
    public void processElement(SessionSignal signal, Context ctx, Collector<Recommendation> out) throws Exception {
        String topProductId = signal.topProductId();

        // if the recommended product is the same, skip it
        if (topProductId.equals(lastRecommendedProduct.value())) return;

        // remove previous timer and register a new one if recommended product changed
        Long oldTimer = pendingTimer.value();
        if (oldTimer != null) ctx.timerService().deleteEventTimeTimer(oldTimer);

        long nextTimer = signal.windowEnd().toEpochMilli() + cooldown.toMillis();
        ctx.timerService().registerEventTimeTimer(nextTimer);

        pendingTimer.update(nextTimer);
        lastRecommendedProduct.update(topProductId);
        out.collect(new Recommendation(signal.shopperId(), topProductId, 0.0, REASON, signal.windowEnd()));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Recommendation> out) throws Exception {
        lastRecommendedProduct.clear();
        pendingTimer.clear();
    }
}