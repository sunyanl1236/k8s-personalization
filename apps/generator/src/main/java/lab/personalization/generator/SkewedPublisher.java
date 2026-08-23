package lab.personalization.generator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// The only component that touches Kafka or real wall-clock delay.
// KafkaProducer is documented thread-safe, so one shared instance across
// all three streams is fine, no need for one per stream.
public class SkewedPublisher {
    private final KafkaProducer<String, byte[]> producer;
    private final ScheduledExecutorService scheduler;

    public SkewedPublisher(KafkaProducer<String, byte[]> producer, ScheduledExecutorService scheduler) {
        this.producer = producer;
        this.scheduler = scheduler;
    }

    public void publish(String topic, String key, byte[] value, Duration delay) {
        Runnable send = () -> producer.send(new ProducerRecord<>(topic, key, value));
        if (delay.isZero()) {
            send.run();
        } else {
            scheduler.schedule(send, delay.toMillis(), TimeUnit.MILLISECONDS);
        }
    }
}
