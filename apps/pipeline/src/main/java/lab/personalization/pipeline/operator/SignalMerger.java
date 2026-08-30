package lab.personalization.pipeline.operator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import lab.personalization.domain.EnrichedClick;
import lab.personalization.domain.RecommendationRequest;
import lab.personalization.domain.ShopperSignal;

public class SignalMerger
        extends KeyedCoProcessFunction<String, ShopperSignal, EnrichedClick, RecommendationRequest> {

    // did the candidate Product find a nearby Product Change?
    public static final OutputTag<RecommendationRequest> UNMATCHED =
            new OutputTag<>("unmatched", TypeInformation.of(RecommendationRequest.class)) {};
    public static final OutputTag<RecommendationRequest> OUT_OF_STOCK =
            new OutputTag<>("out-of-stock", TypeInformation.of(RecommendationRequest.class)) {};

    private static final String REASON_CART_ABANDONED = "cart-abandoned";
    private static final String REASON_PRICE_DROP = "price-drop";
    private static final String REASON_MOST_VIEWED = "most-viewed-in-session";

    private final Duration cooldown;
    private final Duration abandonmentTtl;

    private transient MapState<String, EnrichedClick> matchesByProduct;
    private transient MapState<String, Long> abandonedCarts;
    private transient ValueState<String> lastRecommendedProduct;
    private transient ValueState<Long> pendingTimer;

    public SignalMerger(Duration cooldown, Duration abandonmentTtl) {
        this.cooldown = cooldown;
        this.abandonmentTtl = abandonmentTtl;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        matchesByProduct = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("matches-by-product", Types.STRING,
                        TypeInformation.of(EnrichedClick.class)));
        abandonedCarts = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("abandoned-carts", Types.STRING, Types.LONG));
        lastRecommendedProduct = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-recommended-product", String.class));
        pendingTimer = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pending-timer", Long.class));
    }

    @Override
    public void processElement1(ShopperSignal signal, Context ctx,
                                Collector<RecommendationRequest> out) throws Exception {
        switch (signal.kind()) {
            case CART_ABANDONMENT -> rememberAbandonment(signal, ctx);
            case BROWSING_SESSION -> decide(signal, ctx, out);
        }
    }

    @Override
    public void processElement2(EnrichedClick enriched, Context ctx,
                                Collector<RecommendationRequest> out) throws Exception {
        matchesByProduct.put(enriched.productId(), enriched);
    }

    private void rememberAbandonment(ShopperSignal signal, Context ctx) throws Exception {
        long expiry = signal.eventTime().toEpochMilli() + abandonmentTtl.toMillis();
        abandonedCarts.put(signal.productId(), expiry);
        ctx.timerService().registerEventTimeTimer(expiry);
    }

    private void decide(ShopperSignal session, Context ctx,
                        Collector<RecommendationRequest> out) throws Exception {
        String candidate = session.productId();
        EnrichedClick match = matchesByProduct.get(candidate);

        boolean priceDropped = match != null && match.priceDropped();
        boolean cartAbandoned = abandonedCarts.contains(candidate);

        String reason = cartAbandoned ? REASON_CART_ABANDONED
                : priceDropped ? REASON_PRICE_DROP
                : REASON_MOST_VIEWED;

        RecommendationRequest request = new RecommendationRequest(
                session.shopperId(), candidate, priceDropped, cartAbandoned,
                0.0, reason, session.eventTime());

        // no Recommendation at all for an unbuyable Product, but count the suppression
        if (match != null && match.outOfStock()) {
            ctx.output(OUT_OF_STOCK, request);
            matchesByProduct.clear();
            return;
        }

        // Same Product as the last one recommended to this Shopper, and the cooldown
        // timer has not cleared it yet, so this Browsing Session emits nothing.
        if (candidate.equals(lastRecommendedProduct.value())) {
            matchesByProduct.clear();
            return;
        }

        if (priceDropped || cartAbandoned) {
            armCooldown(session, ctx, candidate);
            out.collect(request);
        } else {
            ctx.output(UNMATCHED, request);
        }

        matchesByProduct.clear();
    }


    private void armCooldown(ShopperSignal session, Context ctx, String candidate) throws Exception {
        Long oldTimer = pendingTimer.value();
        if (oldTimer != null) {
            ctx.timerService().deleteEventTimeTimer(oldTimer);
        }
        long nextTimer = session.eventTime().toEpochMilli() + cooldown.toMillis();
        ctx.timerService().registerEventTimeTimer(nextTimer);
        pendingTimer.update(nextTimer);
        lastRecommendedProduct.update(candidate);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
                        Collector<RecommendationRequest> out) throws Exception {
        Long pending = pendingTimer.value();
        if (pending != null && pending == timestamp) {
            lastRecommendedProduct.clear();
            pendingTimer.clear();
        }

        List<String> expired = new ArrayList<>();
        for (Map.Entry<String, Long> entry : abandonedCarts.entries()) {
            if (entry.getValue() <= timestamp) {
                expired.add(entry.getKey());
            }
        }
        for (String productId : expired) {
            abandonedCarts.remove(productId);
        }
    }
}
