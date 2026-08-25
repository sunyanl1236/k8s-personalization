package lab.personalization.pipeline;

import java.nio.charset.StandardCharsets;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import lab.personalization.domain.JsonCodec;
import lab.personalization.domain.Recommendation;

public class RecommendationSerializationSchema
        implements KafkaRecordSerializationSchema<Recommendation> {

    private final String topic;

    public RecommendationSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            Recommendation recommendation, KafkaSinkContext context, Long timestamp) {
        return new ProducerRecord<>(
                topic,
                null,
                recommendation.generatedAt().toEpochMilli(),
                recommendation.shopperId().getBytes(StandardCharsets.UTF_8),
                JsonCodec.toJson(recommendation));
    }
}
