package lab.personalization.pipeline.operator;

import lab.personalization.domain.PromoRule;
import lab.personalization.domain.RecommendationRequest;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class PromoRuleApplier extends BroadcastProcessFunction<RecommendationRequest, PromoRule, RecommendationRequest> {
    public static final MapStateDescriptor<String, PromoRule> RULE_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("promo-rule", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<PromoRule>() {}));
    private static final String STATE_KEY = "ACTIVE";

    @Override
    public void processElement(RecommendationRequest value, ReadOnlyContext ctx, Collector<RecommendationRequest> out) throws Exception {
        PromoRule rule = ctx.getBroadcastState(RULE_STATE_DESCRIPTOR).get(STATE_KEY);
        if(rule != null && value.priceDropMatched()) {
            out.collect(value.withDiscount(rule.discountPercent()));
        }
        else {
            out.collect(value);
        }
    }

    @Override
    public void processBroadcastElement(PromoRule value, Context ctx, Collector<RecommendationRequest> out) throws Exception {
        ctx.getBroadcastState(RULE_STATE_DESCRIPTOR).put(STATE_KEY, value);
    }
}
