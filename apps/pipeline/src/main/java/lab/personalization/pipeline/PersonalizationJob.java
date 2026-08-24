package lab.personalization.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;

import lab.personalization.domain.Click;

public class PersonalizationJob {

    public static void main(String[] args) throws Exception {
        PipelineConfig config = PipelineConfig.parse(args);

        Configuration flinkConfig = new Configuration();
        flinkConfig.set(PipelineOptions.GENERIC_TYPES, false);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

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

        env.fromSource(source, watermarks, "clickstream")
                .keyBy(Click::shopperId)
                .window(EventTimeSessionWindows.withGap(config.sessionGap()))
                .process(new SessionAggregator())
                .print();

        env.execute("personalization-phase-3");
    }
}
