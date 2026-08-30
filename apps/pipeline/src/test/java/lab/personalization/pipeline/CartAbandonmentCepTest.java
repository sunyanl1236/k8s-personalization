package lab.personalization.pipeline;

import lab.personalization.domain.ActionType;
import lab.personalization.domain.Click;
import lab.personalization.domain.ShopperSignal;
import lab.personalization.domain.SignalKind;
import lab.personalization.pipeline.operator.CartAbandonmentMatcher;
import lab.personalization.pipeline.operator.CartAbandonmentPattern;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CartAbandonmentCepTest {

    private static final Instant T = Instant.parse("2026-08-28T10:00:00Z");
    private static final Duration WITHIN = Duration.ofSeconds(30);

    @Test
    void viewThenCartWithNoCheckoutMatches() throws Exception {
        List<ShopperSignal> matches = runMatches(List.of(
                click("shopper-1", "P1", ActionType.VIEW, 0),
                click("shopper-1", "P1", ActionType.ADD_TO_CART, 3)));

        assertThat(matches).hasSize(1);
        ShopperSignal signal = matches.getFirst();
        assertThat(signal.kind()).isEqualTo(SignalKind.CART_ABANDONMENT);
        assertThat(signal.shopperId()).isEqualTo("shopper-1");
        assertThat(signal.productId()).isEqualTo("P1");
    }

    @Test
    void viewThenCartThenCheckoutDoesNotMatch() throws Exception {
        List<ShopperSignal> matches = runMatches(List.of(
                click("shopper-1", "P1", ActionType.VIEW, 0),
                click("shopper-1", "P1", ActionType.ADD_TO_CART, 3),
                click("shopper-1", "P1", ActionType.CHECKOUT, 5)));

        assertThat(matches).isEmpty();
    }

    @Test
    void aCartOnADifferentProductDoesNotMatch() throws Exception {
        List<ShopperSignal> matches = runMatches(List.of(
                click("shopper-1", "P1", ActionType.VIEW, 0),
                click("shopper-1", "P2", ActionType.ADD_TO_CART, 3)));

        assertThat(matches).isEmpty();
    }

    @Test
    void oneShoppersCartDoesNotCompleteAnothersPattern() throws Exception {
        List<ShopperSignal> matches = runMatches(List.of(
                click("shopper-1", "P1", ActionType.VIEW, 0),
                click("shopper-2", "P1", ActionType.ADD_TO_CART, 3)));

        assertThat(matches).isEmpty();
    }

    @Test
    void aViewThatIsNeverCartedLandsInTheTimedOutSideOutput() throws Exception {
        List<Click> timedOut = runTimedOut(List.of(
                click("shopper-1", "P1", ActionType.VIEW, 0)));

        assertThat(timedOut).hasSize(1);
        assertThat(timedOut.getFirst().productId()).isEqualTo("P1");
    }

    private static Click click(String shopperId, String productId, ActionType action, long offsetSeconds) {
        return new Click(shopperId, productId, T.plusSeconds(offsetSeconds), action);
    }

    private static List<ShopperSignal> runMatches(List<Click> clicks) throws Exception {
        StreamExecutionEnvironment env = newEnv();
        return collect(patternStream(env, clicks));
    }

    private static List<Click> runTimedOut(List<Click> clicks) throws Exception {
        StreamExecutionEnvironment env = newEnv();
        return collect(patternStream(env, clicks).getSideOutput(CartAbandonmentMatcher.TIMED_OUT));
    }

    private static StreamExecutionEnvironment newEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return env;
    }

    private static SingleOutputStreamOperator<ShopperSignal> patternStream(
            StreamExecutionEnvironment env, List<Click> clicks) {

        DataStream<Click> stream = env
                .fromData(clicks, TypeInformation.of(Click.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Click>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((c, ts) -> c.eventTime().toEpochMilli()));

        return CEP.pattern(stream.keyBy(Click::shopperId), CartAbandonmentPattern.pattern(WITHIN))
                .process(new CartAbandonmentMatcher());
    }

    private static <T> List<T> collect(DataStream<T> stream) throws Exception {
        List<T> out = new ArrayList<>();
        try (CloseableIterator<T> it = stream.executeAndCollect()) {
            it.forEachRemaining(out::add);
        }
        return out;
    }
}
