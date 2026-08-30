package lab.personalization.pipeline;

import lab.personalization.domain.EnrichedClick;
import lab.personalization.domain.RecommendationRequest;
import lab.personalization.domain.ShopperSignal;
import lab.personalization.domain.SignalKind;
import lab.personalization.pipeline.operator.SignalMerger;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

class MergeFunctionTest {

    private static final String SHOPPER = "shopper-1";
    private static final Instant T0 = Instant.parse("2026-08-28T10:00:00Z");
    private static final Duration COOLDOWN = Duration.ofSeconds(60);
    private static final Duration TTL = Duration.ofSeconds(60);

    private static final String CART_ABANDONED = "cart-abandoned";
    private static final String PRICE_DROP = "price-drop";
    private static final String MOST_VIEWED = "most-viewed-in-session";

    @Test
    void anEnrichedClickAloneEmitsNothing() throws Exception {
        try (var harness = harness()) {
            harness.processElement2(enriched("P1", 40), ms(1));
            assertThat(harness.extractOutputValues()).isEmpty();
        }
    }

    @Test
    void aMatchedCandidateIsPriceDropped() throws Exception {
        try (var harness = harness()) {
            harness.processElement2(enriched("P1", 40), ms(1));
            harness.processElement1(session("P1", 10), ms(10));

            List<RecommendationRequest> out = harness.extractOutputValues();
            assertThat(out).hasSize(1);
            assertThat(out.getFirst().priceDropMatched()).isTrue();
            assertThat(out.getFirst().reason()).isEqualTo(PRICE_DROP);
            assertThat(out.getFirst().candidateProductId()).isEqualTo("P1");
            assertThat(out.getFirst().discountPercent()).isZero();
        }
    }

    @Test
    void anUnmatchedCandidateGoesToTheUnmatchedSideOutput() throws Exception {
        try (var harness = harness()) {
            harness.processElement1(session("P9", 10), ms(10));

            assertThat(harness.extractOutputValues()).isEmpty();
            List<RecommendationRequest> unmatched = side(harness, SignalMerger.UNMATCHED);
            assertThat(unmatched).hasSize(1);
            assertThat(unmatched.getFirst().candidateProductId()).isEqualTo("P9");
            assertThat(unmatched.getFirst().reason()).isEqualTo(MOST_VIEWED);
        }
    }

    @Test
    void anOutOfStockCandidateIsSuppressedToItsOwnSideOutput() throws Exception {
        try (var harness = harness()) {
            harness.processElement2(enriched("P1", 0), ms(1));
            harness.processElement1(session("P1", 10), ms(10));

            assertThat(harness.extractOutputValues()).isEmpty();
            assertThat(side(harness, SignalMerger.UNMATCHED)).isEmpty();

            List<RecommendationRequest> suppressed = side(harness, SignalMerger.OUT_OF_STOCK);
            assertThat(suppressed).hasSize(1);
            assertThat(suppressed.getFirst().candidateProductId()).isEqualTo("P1");
            assertThat(suppressed.getFirst().priceDropMatched()).isTrue();
            assertThat(suppressed.getFirst().reason()).isEqualTo(PRICE_DROP);
        }
    }

    @Test
    void aSuppressedRequestStillReportsWhyItWouldHaveBeenRecommended() throws Exception {
        try (var harness = harness()) {
            harness.processElement2(enriched("P1", 0), ms(1));
            harness.processElement1(abandoned("P1", 5), ms(5));
            harness.processElement1(session("P1", 10), ms(10));

            assertThat(harness.extractOutputValues()).isEmpty();

            List<RecommendationRequest> suppressed = side(harness, SignalMerger.OUT_OF_STOCK);
            assertThat(suppressed).hasSize(1);
            assertThat(suppressed.getFirst().cartAbandoned()).isTrue();
            assertThat(suppressed.getFirst().reason()).isEqualTo(CART_ABANDONED);
        }
    }

    @Test
    void cartAbandonedOutranksPriceDrop() throws Exception {
        try (var harness = harness()) {
            harness.processElement2(enriched("P1", 40), ms(1));
            harness.processElement1(abandoned("P1", 5), ms(5));
            harness.processElement1(session("P1", 10), ms(10));

            List<RecommendationRequest> out = harness.extractOutputValues();
            assertThat(out).hasSize(1);
            assertThat(out.getFirst().reason()).isEqualTo(CART_ABANDONED);
            assertThat(out.getFirst().cartAbandoned()).isTrue();
            assertThat(out.getFirst().priceDropMatched()).isTrue();
        }
    }

    @Test
    void priceDropMatchesClearOnSessionCloseButAbandonmentsSurvive() throws Exception {
        try (var harness = harness()) {
            harness.processElement2(enriched("P1", 40), ms(1));
            harness.processElement2(enriched("P2", 40), ms(2));
            harness.processElement1(abandoned("P3", 3), ms(3));

            harness.processElement1(session("P1", 10), ms(10));
            harness.processElement1(session("P2", 20), ms(20));
            harness.processElement1(session("P3", 30), ms(30));

            List<RecommendationRequest> out = harness.extractOutputValues();
            assertThat(out).hasSize(2);

            assertThat(out.get(0).candidateProductId()).isEqualTo("P1");
            assertThat(out.get(0).reason()).isEqualTo(PRICE_DROP);

            assertThat(out.get(1).candidateProductId()).isEqualTo("P3");
            assertThat(out.get(1).reason()).isEqualTo(CART_ABANDONED);

            List<RecommendationRequest> unmatched = side(harness, SignalMerger.UNMATCHED);
            assertThat(unmatched).hasSize(1);
            assertThat(unmatched.getFirst().candidateProductId()).isEqualTo("P2");
        }
    }

    @Test
    void anAbandonmentExpiresAfterItsTtl() throws Exception {
        try (var harness = harness()) {
            harness.processElement1(abandoned("P1", 0), ms(0));
            advanceWatermark(harness, ms(0) + TTL.toMillis() + 1);
            harness.processElement1(session("P1", 200), ms(200));

            assertThat(harness.extractOutputValues()).isEmpty();
            List<RecommendationRequest> unmatched = side(harness, SignalMerger.UNMATCHED);
            assertThat(unmatched).hasSize(1);
            assertThat(unmatched.getFirst().reason()).isEqualTo(MOST_VIEWED);
        }
    }

    @Test
    void theSameProductIsNotRecommendedTwiceWithinTheCooldown() throws Exception {
        try (var harness = harness()) {
            harness.processElement2(enriched("P1", 40), ms(1));
            harness.processElement1(session("P1", 10), ms(10));

            harness.processElement2(enriched("P1", 40), ms(15));
            harness.processElement1(session("P1", 20), ms(20));

            assertThat(harness.extractOutputValues()).hasSize(1);
        }
    }

    private static KeyedTwoInputStreamOperatorTestHarness<String, ShopperSignal, EnrichedClick, RecommendationRequest>
            harness() throws Exception {
        var harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                new SignalMerger(COOLDOWN, TTL),
                ShopperSignal::shopperId, EnrichedClick::shopperId, Types.STRING);
        harness.open();
        return harness;
    }

    private static void advanceWatermark(
            KeyedTwoInputStreamOperatorTestHarness<String, ShopperSignal, EnrichedClick, RecommendationRequest> harness,
            long timestamp) throws Exception {
        harness.processWatermark1(new Watermark(timestamp));
        harness.processWatermark2(new Watermark(timestamp));
    }

    private static <T> List<T> side(
            KeyedTwoInputStreamOperatorTestHarness<String, ShopperSignal, EnrichedClick, RecommendationRequest> harness,
            OutputTag<T> tag) {
        Queue<StreamRecord<T>> records = harness.getSideOutput(tag);
        return records == null ? List.of() : records.stream().map(StreamRecord::getValue).toList();
    }

    private static long ms(long offsetSeconds) {
        return T0.plusSeconds(offsetSeconds).toEpochMilli();
    }

    private static EnrichedClick enriched(String productId, int stock) {
        return new EnrichedClick(SHOPPER, productId, T0, 19.99, 24.99, stock, T0.plusSeconds(1));
    }

    private static ShopperSignal session(String candidateProductId, long offsetSeconds) {
        return new ShopperSignal(SHOPPER, SignalKind.BROWSING_SESSION,
                T0.plusSeconds(offsetSeconds), candidateProductId);
    }

    private static ShopperSignal abandoned(String productId, long offsetSeconds) {
        return new ShopperSignal(SHOPPER, SignalKind.CART_ABANDONMENT,
                T0.plusSeconds(offsetSeconds), productId);
    }
}
