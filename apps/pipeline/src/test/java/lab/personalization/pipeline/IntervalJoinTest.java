package lab.personalization.pipeline;

import lab.personalization.domain.ActionType;
import lab.personalization.domain.Click;
import lab.personalization.domain.EnrichedClick;
import lab.personalization.domain.ProductChange;
import lab.personalization.pipeline.operator.ProductChangeJoiner;

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

class IntervalJoinTest {

    private static final Instant T = Instant.parse("2026-08-28T10:00:00Z");

    @Test
    void clickWithinTwoSecondsOfProductChangeMatches() throws Exception {
        List<EnrichedClick> joined = runJoin(
                List.of(click("P1", T)),
                List.of(change("P1", T.plusSeconds(1), 19.99, 24.99, 40)));

        assertThat(joined).hasSize(1);
        assertThat(joined.getFirst().productId()).isEqualTo("P1");
    }

    @Test
    void clickFourSecondsBeforeProductChangeDoesNotMatch() throws Exception {
        List<EnrichedClick> joined = runJoin(
                List.of(click("P2", T)),
                List.of(change("P2", T.plusSeconds(4), 19.99, 24.99, 40)));

        assertThat(joined).isEmpty();
    }

    @Test
    void aChangeOnAnotherProductDoesNotMatch() throws Exception {
        List<EnrichedClick> joined = runJoin(
                List.of(click("P1", T)),
                List.of(change("P3", T.plusSeconds(1), 19.99, 24.99, 40)));

        assertThat(joined).isEmpty();
    }

    @Test
    void theEnrichedClickCarriesEverythingTheMergeNeeds() throws Exception {
        List<EnrichedClick> joined = runJoin(
                List.of(click("P1", T)),
                List.of(change("P1", T.plusSeconds(1), 19.99, 24.99, 0)));

        assertThat(joined).hasSize(1);
        EnrichedClick enriched = joined.getFirst();

        assertThat(enriched.shopperId()).isEqualTo("shopper-1");
        assertThat(enriched.productId()).isEqualTo("P1");
        assertThat(enriched.clickTime()).isEqualTo(T);
        assertThat(enriched.changeTime()).isEqualTo(T.plusSeconds(1));
        assertThat(enriched.price()).isEqualTo(19.99);
        assertThat(enriched.previousPrice()).isEqualTo(24.99);
        assertThat(enriched.stock()).isZero();

        assertThat(enriched.priceDropped()).isTrue();
        assertThat(enriched.outOfStock()).isTrue();
    }

    @Test
    void oneClickNearTwoChangesProducesTwoEnrichedClicks() throws Exception {
        List<EnrichedClick> joined = runJoin(
                List.of(click("P1", T)),
                List.of(change("P1", T.minusSeconds(2), 21.00, 24.99, 40),
                        change("P1", T.plusSeconds(2), 19.99, 21.00, 40)));

        assertThat(joined).hasSize(2);
    }

    private static Click click(String productId, Instant eventTime) {
        return new Click("shopper-1", productId, eventTime, ActionType.VIEW);
    }

    private static ProductChange change(String productId, Instant eventTime,
                                        double price, double previousPrice, int stock) {
        return new ProductChange(productId, eventTime, price, previousPrice, stock, stock);
    }

    private static List<EnrichedClick> runJoin(List<Click> clicks, List<ProductChange> changes)
            throws Exception {

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

        DataStream<EnrichedClick> joined = clickStream
                .keyBy(Click::productId)
                .intervalJoin(changeStream.keyBy(ProductChange::productId))
                .between(Duration.ofSeconds(-2), Duration.ofSeconds(2))
                .process(new ProductChangeJoiner());

        List<EnrichedClick> collected = new ArrayList<>();
        try (CloseableIterator<EnrichedClick> it = joined.executeAndCollect()) {
            it.forEachRemaining(collected::add);
        }
        return collected;
    }
}
