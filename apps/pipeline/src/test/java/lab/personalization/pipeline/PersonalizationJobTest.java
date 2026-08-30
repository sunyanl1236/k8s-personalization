package lab.personalization.pipeline;

import lab.personalization.domain.ActionType;
import lab.personalization.domain.Click;
import lab.personalization.domain.ProductChange;
import lab.personalization.domain.PromoRule;
import lab.personalization.domain.Recommendation;
import lab.personalization.pipeline.service.DeterministicMockClient;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The only test that sees the wiring: union, re-key, broadcast connect, async stage.
 * Every other test exercises one operator in isolation.
 */
class PersonalizationJobTest {

    private static final Instant T0 = Instant.parse("2026-08-30T10:00:00Z");
    private static final String SHOPPER = "shopper-1";

    @Test
    void oneBrowsingSessionProducesOneDiscountedRecommendation() throws Exception {
        // A far-future Click from another Shopper pushes the watermark past shopper-1's
        // window end, so shopper-1's session closes DURING the stream rather than at
        // MAX_WATERMARK. Without it, the window and the join would both fire at end of
        // input and their arrival order at the merge would be a race.
        List<Click> clicks = List.of(
                click(SHOPPER, "P1", ActionType.VIEW, 0),
                click(SHOPPER, "P1", ActionType.VIEW, 2),
                click("shopper-2", "P2", ActionType.VIEW, 200));   // watermark pusher

        List<ProductChange> changes = List.of(
                // 1s from the Click at +0, inside the +/-2s join window. Price falls
                // 24.99 -> 19.99 and stock is non-zero: a real drop on a buyable Product.
                new ProductChange("P1", T0.plusSeconds(1), 19.99, 24.99, 40, 40));

        List<Recommendation> out = run(clicks, changes,
                List.of(new PromoRule("promo-1", "10% off", 10.0)));

        // shopper-2's candidate P2 never matched, so it goes to UNMATCHED, not the sink.
        assertThat(out).hasSize(1);
        Recommendation only = out.getFirst();

        assertThat(only.shopperId()).isEqualTo(SHOPPER);
        assertThat(only.productId()).isEqualTo("SUGGESTED");          // the service answered
        assertThat(only.discountPercent()).isEqualTo(10.0);           // broadcast rule applied
        assertThat(only.reason()).isEqualTo("price-drop");            // the join reached the merge
        assertThat(only.generatedAt()).isEqualTo(T0.plusSeconds(8));  // window end, not wall clock
    }

    private static Click click(String shopperId, String productId, ActionType action, long offsetSeconds) {
        return new Click(shopperId, productId, T0.plusSeconds(offsetSeconds), action);
    }

    private static List<Recommendation> run(List<Click> clicks,
                                            List<ProductChange> changes,
                                            List<PromoRule> rules) throws Exception {
        PipelineConfig config = PipelineConfig.parse(new String[]{
                "--debug-prints=false",
                "--recommendation-catalogue=SUGGESTED",   // one entry, so the answer is fixed
                "--recommendation-latency-ms=5"
        });

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Click> clickStream = env
                .fromData(clicks, TypeInformation.of(Click.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Click>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((c, ts) -> c.eventTime().toEpochMilli()));

        DataStream<ProductChange> changeStream = env
                .fromData(changes, TypeInformation.of(ProductChange.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ProductChange>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((c, ts) -> c.eventTime().toEpochMilli()));

        List<String> catalogue = config.recommendationCatalogue();
        Duration latency = config.recommendationLatency();

        DataStream<Recommendation> out = PersonalizationJob.buildGraph(
                clickStream, changeStream,
                env.fromData(rules, TypeInformation.of(PromoRule.class)),
                config,
                () -> new DeterministicMockClient(catalogue, latency));

        List<Recommendation> collected = new ArrayList<>();
        try (CloseableIterator<Recommendation> it = out.executeAndCollect()) {
            it.forEachRemaining(collected::add);
        }
        return collected;
    }
}
