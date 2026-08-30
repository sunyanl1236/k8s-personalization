package lab.personalization.pipeline;

import lab.personalization.domain.PromoRule;
import lab.personalization.domain.RecommendationRequest;
import lab.personalization.pipeline.operator.PromoRuleApplier;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.streaming.util.BroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PromoRuleBroadcastTest {

    private static final Instant T0 = Instant.parse("2026-08-29T10:00:00Z");

    @Test
    void withNoRuleYetAMatchedRequestStillEmitsAtZero() throws Exception {
        try (var harness = harness()) {
            harness.processElement(matched(), 10L);

            List<RecommendationRequest> out = harness.extractOutputValues();
            assertThat(out).hasSize(1);
            assertThat(out.getFirst().discountPercent()).isZero();
        }
    }

    @Test
    void aMatchedRequestCarriesTheActiveRuleDiscount() throws Exception {
        try (var harness = harness()) {
            harness.processBroadcastElement(new PromoRule("promo-1", "10% off", 10.0), 1L);
            harness.processElement(matched(), 10L);

            List<RecommendationRequest> out = harness.extractOutputValues();
            assertThat(out).hasSize(1);
            assertThat(out.getFirst().discountPercent()).isEqualTo(10.0);
            assertThat(out.getFirst().reason()).isEqualTo("price-drop");
        }
    }

    @Test
    void aNewRuleReplacesTheOldOneAndStateHoldsExactlyOneEntry() throws Exception {
        try (var harness = harness()) {
            harness.processBroadcastElement(new PromoRule("promo-1", "10% off", 10.0), 1L);
            harness.processElement(matched(), 10L);

            harness.processBroadcastElement(new PromoRule("promo-2", "15% off", 15.0), 20L);
            harness.processElement(matched(), 30L);

            List<RecommendationRequest> out = harness.extractOutputValues();
            assertThat(out).hasSize(2);
            assertThat(out.get(0).discountPercent()).isEqualTo(10.0);
            assertThat(out.get(1).discountPercent()).isEqualTo(15.0);

            assertThat(entryCount(harness)).isEqualTo(1);
        }
    }

    @Test
    void anUnmatchedRequestStaysAtZeroRegardlessOfTheRule() throws Exception {
        try (var harness = harness()) {
            harness.processBroadcastElement(new PromoRule("promo-1", "10% off", 10.0), 1L);
            harness.processElement(unmatched(), 10L);

            List<RecommendationRequest> out = harness.extractOutputValues();
            assertThat(out).hasSize(1);
            assertThat(out.getFirst().discountPercent()).isZero();
            assertThat(out.getFirst().reason()).isEqualTo("most-viewed-in-session");
        }
    }

    @Test
    void cartAbandonedAloneDoesNotEarnTheDiscount() throws Exception {
        try (var harness = harness()) {
            harness.processBroadcastElement(new PromoRule("promo-1", "10% off", 10.0), 1L);
            harness.processElement(cartAbandonedOnly(), 10L);

            List<RecommendationRequest> out = harness.extractOutputValues();
            assertThat(out).hasSize(1);
            assertThat(out.getFirst().cartAbandoned()).isTrue();
            assertThat(out.getFirst().discountPercent()).isZero();
        }
    }

    private static BroadcastOperatorTestHarness<RecommendationRequest, PromoRule, RecommendationRequest>
            harness() throws Exception {
        var harness = ProcessFunctionTestHarnesses.forBroadcastProcessFunction(
                new PromoRuleApplier(), PromoRuleApplier.RULE_STATE_DESCRIPTOR);
        harness.open();
        return harness;
    }

    private static int entryCount(
            BroadcastOperatorTestHarness<RecommendationRequest, PromoRule, RecommendationRequest> harness)
            throws Exception {
        BroadcastState<String, PromoRule> state = harness.getBroadcastState(PromoRuleApplier.RULE_STATE_DESCRIPTOR);
        int count = 0;
        for (Map.Entry<String, PromoRule> ignored : state.immutableEntries()) {
            count++;
        }
        return count;
    }

    private static RecommendationRequest matched() {
        return new RecommendationRequest("shopper-1", "P1", true, false, 0.0, "price-drop", T0);
    }

    private static RecommendationRequest unmatched() {
        return new RecommendationRequest("shopper-1", "P1", false, false, 0.0, "most-viewed-in-session", T0);
    }

    private static RecommendationRequest cartAbandonedOnly() {
        return new RecommendationRequest("shopper-1", "P1", false, true, 0.0, "cart-abandoned", T0);
    }
}
