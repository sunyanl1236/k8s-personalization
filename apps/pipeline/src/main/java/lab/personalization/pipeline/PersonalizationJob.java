package lab.personalization.pipeline;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import java.util.List;
import lab.personalization.pipeline.operator.AsyncRecommendationLookup;
import lab.personalization.pipeline.service.DeterministicMockClient;
import lab.personalization.pipeline.service.RecommendationClientFactory;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;

import lab.personalization.domain.Click;
import lab.personalization.domain.EnrichedClick;
import lab.personalization.domain.ProductChange;
import lab.personalization.domain.PromoRule;
import lab.personalization.domain.Recommendation;
import lab.personalization.domain.RecommendationRequest;
import lab.personalization.domain.SessionSignal;
import lab.personalization.domain.ShopperSignal;
import lab.personalization.pipeline.operator.CartAbandonmentMatcher;
import lab.personalization.pipeline.operator.CartAbandonmentPattern;
import lab.personalization.pipeline.operator.ProductChangeJoiner;
import lab.personalization.pipeline.operator.PromoRuleApplier;
import lab.personalization.pipeline.operator.SessionAggregator;
import lab.personalization.pipeline.operator.SignalMerger;
import lab.personalization.pipeline.serde.ClickDeserializationSchema;
import lab.personalization.pipeline.serde.ProductChangeDeserializationSchema;
import lab.personalization.pipeline.serde.PromoRuleDeserializationSchema;
import lab.personalization.pipeline.serde.RecommendationSerializationSchema;

public class PersonalizationJob {

    public static void main(String[] args) throws Exception {
        PipelineConfig config = PipelineConfig.parse(args);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(flinkConfiguration(config));

        DataStream<Click> clicks = env.fromSource(
                KafkaSources.of(config, config.inputTopic(), "", new ClickDeserializationSchema()),
                eventTimeWatermarks(config, (Click click, long ts) -> click.eventTime().toEpochMilli()),
                "click-stream");

        DataStream<ProductChange> productChanges = env.fromSource(
                KafkaSources.of(config, config.productChangeTopic(), "-product-change",
                        new ProductChangeDeserializationSchema()),
                eventTimeWatermarks(config, (ProductChange change, long ts) -> change.eventTime().toEpochMilli()),
                "product-change-stream");

        // Promo Rules are not watermark-gated, which is why PromoRule has no eventTime.
        // A real strategy here would hold the job back on a stream that emits every 30s.
        DataStream<PromoRule> promoRules = env.fromSource(
                KafkaSources.of(config, config.promoRuleTopic(), "-promo-rule",
                        new PromoRuleDeserializationSchema()),
                WatermarkStrategy.noWatermarks(), "promo-rule-stream");

        DataStream<Recommendation> recommendations =
                buildGraph(clicks, productChanges, promoRules, config, clientFactory(config));

        recommendations.sinkTo(recommendationSink(config));

        env.execute("personalization-phase-4");
    }

    // The whole graph, taking streams rather than Kafka sources so a test can feed it
    // bounded input. Returns the stream the sink consumes.
    static DataStream<Recommendation> buildGraph(
            DataStream<Click> clicks,
            DataStream<ProductChange> productChanges,
            DataStream<PromoRule> promoRules,
            PipelineConfig config,
            RecommendationClientFactory clientFactory) {

        // One shuffle by shopperId, feeding both Shopper-keyed branches.
        KeyedStream<Click, String> byShopper = clicks.keyBy(Click::shopperId);

        SingleOutputStreamOperator<SessionSignal> sessionSignals = sessionBranch(byShopper, config);
        SingleOutputStreamOperator<ShopperSignal> cartAbandonments = cepBranch(byShopper, config);
        SingleOutputStreamOperator<EnrichedClick> priceDropMatches =
                productChangeBranch(clicks, productChanges, config);

        DataStream<ShopperSignal> shopperSignals = sessionSignals
                .map(ShopperSignal::browsingSession).returns(ShopperSignal.class)
                .union(cartAbandonments);

        SingleOutputStreamOperator<RecommendationRequest> requests =
                merge(shopperSignals, priceDropMatches, config);

        SingleOutputStreamOperator<RecommendationRequest> priced = requests
                .connect(promoRules.broadcast(PromoRuleApplier.RULE_STATE_DESCRIPTOR))
                .process(new PromoRuleApplier());

        // Its own operator, downstream of the merge: a KeyedCoProcessFunction cannot make
        // this call and stay correct. orderedWait, because unordered reorders records
        // between watermarks and Phase 3's restart Drill asserts identical output.
        SingleOutputStreamOperator<Recommendation> recommendations = AsyncDataStream.orderedWait(
                priced,
                new AsyncRecommendationLookup(clientFactory),
                config.recommendationTimeout().toMillis(), TimeUnit.MILLISECONDS,
                config.recommendationCapacity());

        if (config.debugPrints()) {
            printDebugStreams(productChanges, sessionSignals, cartAbandonments, shopperSignals,
                    priceDropMatches, requests, priced, recommendations);
        }
        return recommendations;
    }

    private static SingleOutputStreamOperator<SessionSignal> sessionBranch(
            KeyedStream<Click, String> byShopper, PipelineConfig config) {
        return byShopper
                .window(EventTimeSessionWindows.withGap(config.sessionGap()))
                .sideOutputLateData(SessionAggregator.LATE_CLICKS)
                .process(new SessionAggregator());
    }

    private static SingleOutputStreamOperator<ShopperSignal> cepBranch(
            KeyedStream<Click, String> byShopper, PipelineConfig config) {
        return CEP.pattern(byShopper, CartAbandonmentPattern.pattern(config.cepWithin()))
                .process(new CartAbandonmentMatcher());
    }

    // Forked from the raw stream in parallel with keyBy(shopperId), never below it, and never
    // filtered to price drops: the merge needs stock from every update. ADR 0003 and 0008.
    private static SingleOutputStreamOperator<EnrichedClick> productChangeBranch(
            DataStream<Click> clicks, DataStream<ProductChange> productChanges,
            PipelineConfig config) {
        return clicks
                .keyBy(Click::productId)
                .intervalJoin(productChanges.keyBy(ProductChange::productId))
                .between(config.joinLowerBound(), config.joinUpperBound())
                .process(new ProductChangeJoiner());
    }

    // The re-key is what physically moves each match to the worker holding the session.
    private static SingleOutputStreamOperator<RecommendationRequest> merge(
            DataStream<ShopperSignal> shopperSignals,
            SingleOutputStreamOperator<EnrichedClick> priceDropMatches, PipelineConfig config) {
        return shopperSignals.keyBy(ShopperSignal::shopperId)
                .connect(priceDropMatches.keyBy(EnrichedClick::shopperId))
                .process(new SignalMerger(config.cooldown(), config.abandonmentTtl()));
    }

    // Captures only the catalogue and the latency, both serializable. A client cannot be
    // captured directly: it owns an executor and would never reach a TaskManager.
    private static RecommendationClientFactory clientFactory(PipelineConfig config) {
        List<String> catalogue = config.recommendationCatalogue();
        Duration latency = config.recommendationLatency();
        return () -> new DeterministicMockClient(catalogue, latency);
    }

    private static KafkaSink<Recommendation> recommendationSink(PipelineConfig config) {
        return KafkaSink.<Recommendation>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(new RecommendationSerializationSchema(config.outputTopic()))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(config.transactionalIdPrefix())
                .setProperty("transaction.timeout.ms",
                        String.valueOf(config.transactionTimeout().toMillis()))
                .build();
    }

    // Without withIdleness the job produces nothing at all, because parallelism far exceeds
    // the partition count and idle subtasks pin the watermark at Long.MIN_VALUE.
    private static <T> WatermarkStrategy<T> eventTimeWatermarks(
            PipelineConfig config, SerializableTimestampAssigner<T> assigner) {
        return WatermarkStrategy.<T>forBoundedOutOfOrderness(config.watermarkBound())
                .withTimestampAssigner(assigner)
                .withIdleness(config.watermarkIdleness());
    }

    private static Configuration flinkConfiguration(PipelineConfig config) {
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration(config.flinkConfDir());

        if (flinkConfig.getString("state.backend.type", null) == null) {
            throw new IllegalStateException("no config.yaml loaded from " + config.flinkConfDir());
        }

        flinkConfig.setString("s3.access-key", Env.require("MINIO_ACCESS_KEY"));
        flinkConfig.setString("s3.secret-key", Env.require("MINIO_SECRET_KEY"));

        if (config.restoreFrom() != null) {
            flinkConfig.set(StateRecoveryOptions.SAVEPOINT_PATH, config.restoreFrom());
        }

        FileSystem.initialize(flinkConfig, null);
        return flinkConfig;
    }

    private static void printDebugStreams(
            DataStream<ProductChange> productChanges,
            SingleOutputStreamOperator<SessionSignal> sessionSignals,
            SingleOutputStreamOperator<ShopperSignal> cartAbandonments,
            DataStream<ShopperSignal> shopperSignals,
            SingleOutputStreamOperator<EnrichedClick> priceDropMatches,
            SingleOutputStreamOperator<RecommendationRequest> requests,
            SingleOutputStreamOperator<RecommendationRequest> priced,
            DataStream<Recommendation> recommendations) {

        productChanges.print("PRODUCT-CHANGE");
        sessionSignals.print("SIGNAL");
        sessionSignals.getSideOutput(SessionAggregator.LATE_CLICKS).print("LATE");
        cartAbandonments.print("CART-ABANDONED");
        cartAbandonments.getSideOutput(CartAbandonmentMatcher.TIMED_OUT).print("CEP-TIMEOUT");
        shopperSignals.print("MERGED-SIGNAL");
        priceDropMatches.print("PRICE-DROP-MATCH");
        requests.print("REQUEST");
        requests.getSideOutput(SignalMerger.UNMATCHED).print("UNMATCHED");
        requests.getSideOutput(SignalMerger.OUT_OF_STOCK).print("OUT-OF-STOCK");
        priced.print("PROMO-RULE");
        recommendations.print("RECOMMEND");
    }
}
