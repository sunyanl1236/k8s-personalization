package lab.personalization.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.util.OutputTag;

import lab.personalization.domain.Click;
import lab.personalization.domain.Recommendation;
import lab.personalization.domain.SessionSignal;

public class PersonalizationJob {
    static final OutputTag<Click> LATE_CLICKS = new OutputTag<>("late-clicks", TypeInformation.of(Click.class)) {};

    public static void main(String[] args) throws Exception {
        PipelineConfig config = PipelineConfig.parse(args);

        Configuration flinkConfig = GlobalConfiguration.loadConfiguration(config.flinkConfDir());

        if (flinkConfig.getString("state.backend.type", null) == null) {
            throw new IllegalStateException("no config.yaml loaded from " + config.flinkConfDir());
        }

        flinkConfig.setString("s3.access-key", Env.require("MINIO_ACCESS_KEY"));
        flinkConfig.setString("s3.secret-key", Env.require("MINIO_SECRET_KEY"));

        FileSystem.initialize(flinkConfig, null);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

        env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        KafkaSource<Click> source = KafkaSource.<Click>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.inputTopic())
                .setGroupId(config.consumerGroup())
                .setStartingOffsets(config.startFromEarliest()
                        ? OffsetsInitializer.earliest()
                        : OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new ClickDeserializationSchema())
                .build();

        WatermarkStrategy<Click> watermarks = WatermarkStrategy
        .<Click>forBoundedOutOfOrderness(config.watermarkBound())
        .withTimestampAssigner((click, recordTimestamp) -> click.eventTime().toEpochMilli())
                // Without this the job produces nothing at all because most parallelism is idle
                .withIdleness(config.watermarkIdleness());

        SingleOutputStreamOperator<SessionSignal> signals = env.fromSource(source, watermarks, "clickstream")
                .keyBy(Click::shopperId)
                .window(EventTimeSessionWindows.withGap(config.sessionGap()))
                .sideOutputLateData(LATE_CLICKS)
                .process(new SessionAggregator());
        
        SingleOutputStreamOperator<Recommendation> recommends = signals.keyBy(SessionSignal::shopperId)
               .process(new RecommendationDecider(config.cooldown()));

        KafkaSink<Recommendation> sink = KafkaSink.<Recommendation>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(new RecommendationSerializationSchema(config.outputTopic()))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(config.transactionalIdPrefix())
                .setProperty("transaction.timeout.ms",
                        String.valueOf(config.transactionTimeout().toMillis()))
                .build();
        
        recommends.sinkTo(sink);

        signals.getSideOutput(LATE_CLICKS).print("LATE");
        signals.print("SIGNAL");
        recommends.print("RECOMMEND");

        env.execute("personalization-phase-3");
    }
}
