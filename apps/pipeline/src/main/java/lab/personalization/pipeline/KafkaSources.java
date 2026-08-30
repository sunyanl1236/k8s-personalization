package lab.personalization.pipeline;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

final class KafkaSources {

    static <T> KafkaSource<T> of(PipelineConfig config, String topic, String groupSuffix,
                                 DeserializationSchema<T> deserializer) {
        KafkaSourceBuilder<T> builder = KafkaSource.<T>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(topic)
                .setGroupId(config.consumerGroup() + groupSuffix)
                .setStartingOffsets(config.startFromEarliest()
                        ? OffsetsInitializer.earliest()
                        : OffsetsInitializer.latest())
                .setValueOnlyDeserializer(deserializer);

        if (config.bounded()) {
            builder.setBounded(OffsetsInitializer.latest());
        }
        return builder.build();
    }

    private KafkaSources() {}
}
