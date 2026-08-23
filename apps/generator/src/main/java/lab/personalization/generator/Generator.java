package lab.personalization.generator;

import java.time.Duration;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import lab.personalization.domain.Click;
import lab.personalization.domain.JsonCodec;
import lab.personalization.domain.ProductChange;
import lab.personalization.domain.PromoRule;
import lab.personalization.generator.factory.Catalog;
import lab.personalization.generator.factory.ClickFactory;
import lab.personalization.generator.factory.ProductChangeFactory;
import lab.personalization.generator.factory.PromoRuleFactory;

public class Generator {
    public static void main(String[] args) {
        GeneratorConfig config = GeneratorConfig.parse(args);
        Random random = new Random();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // StringSerializer turn a String into bytes
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()); // JsonCodec as byte[] already, so there's nothing left to convert, this one's a formality
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        SkewedPublisher publisher = new SkewedPublisher(producer, scheduler);

        ClickFactory clickFactory = new ClickFactory(Catalog.SHOPPER_IDS, Catalog.PRODUCT_IDS, random);
        ProductChangeFactory productChangeFactory = new ProductChangeFactory(Catalog.PRODUCT_IDS, random);
        PromoRuleFactory promoRuleFactory = new PromoRuleFactory(random);

        SkewedEventStream<Click> clicks = new SkewedEventStream<>(
                clickFactory::next,
                Click::shopperId,
                JsonCodec::toJson,
                "clickstream",
                config.clickMaxSkew(),
                publisher,
                random);

        SkewedEventStream<ProductChange> productChanges = new SkewedEventStream<>(
                productChangeFactory::next,
                ProductChange::productId,
                JsonCodec::toJson,
                "product-change",
                config.productChangeMaxSkew(),
                publisher,
                random);

        SkewedEventStream<PromoRule> promoRules = new SkewedEventStream<>(
                promoRuleFactory::next,
                PromoRule::ruleId,
                JsonCodec::toJson,
                "promo-rule",
                Duration.ZERO,
                publisher,
                random);

        clicks.start(config.clickEventsPerSecond());
        productChanges.start(config.productChangeEventsPerSecond());
        promoRules.start(1.0 / config.promoRuleInterval().toSeconds());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            clicks.stop();
            productChanges.stop();
            promoRules.stop();
            // send() only buffers, it doesn't wait for Kafka's acknowledgment
            // flush() forces every buffered record out immediately and blocks until each one is acknowledged or fails
            // close() then blocks until any remaining in-flight requests finish and releases the producer's threads and connections.
            // Calling both, in that order, is what makes Ctrl+C a clean stop rather than a silent drop of the last few events.
            producer.flush();
            producer.close();
            scheduler.shutdown();
        }));

        System.out.println("Generator running. Ctrl+C to stop.");
    }
}
