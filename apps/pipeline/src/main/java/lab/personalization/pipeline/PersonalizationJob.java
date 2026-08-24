package lab.personalization.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;

import lab.personalization.domain.Click;

// Phase 3's job, at Task 3: read Clicks and print them. Nothing more, on
// purpose.
//
// This stage answers three questions that have three different failure
// signatures: do the Flink 2.2 dependencies resolve, do the Phase 2 records
// survive Flink's type system, and does the external Kafka listener work from
// inside a Flink source. Adding windowing before knowing those would make one
// failure look like another.
//
// Watermarks arrive in Task 4, the session window in Task 4, the Late Click
// side output in Task 5, the RecommendationDecider in Task 6, and the sink in
// Task 8.
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
                // Explicit, and load-bearing for Task 9. The default is the
                // consumer group's committed offsets, which would make the
                // Drill's second run start where the first stopped and read
                // nothing. This does not conflict with --restore-from: offsets
                // held in a restored checkpoint take priority over the
                // initializer, and that priority is what recovery depends on.
                .setStartingOffsets(config.startFromEarliest()
                        ? OffsetsInitializer.earliest()
                        : OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new ClickDeserializationSchema())
                .build();

        WatermarkStrategy<Click> watermarks = WatermarkStrategy
        .<Click>forBoundedOutOfOrderness(config.watermarkBound())
        .withTimestampAssigner((click, recordTimestamp) -> click.eventTime().toEpochMilli())
                // Without this the job produces nothing at all. The watermark
                // is the MINIMUM across inputs, and the 13 source subtasks
                // that get no Kafka partition (parallelism 16, 3 partitions)
                // never emit one, pinning it at Long.MIN_VALUE. See
                // PipelineConfig.watermarkIdleness.
                .withIdleness(config.watermarkIdleness());

        // noWatermarks is a placeholder that Task 4 replaces with bounded
        // out-of-orderness. Nothing here consumes event time yet, and claiming
        // a watermark strategy this stage does not use would be a lie in the
        // job graph.
        env.fromSource(source, watermarks, "clickstream")
                .keyBy(Click::shopperId)
                .window(EventTimeSessionWindows.withGap(config.sessionGap()))
                .process(new SessionAggregator())
                .print();

        env.execute("personalization-phase-3");
    }
}
