package lab.personalization.generator;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

// One reusable driver for all three streams, per the design doc: Click and
// ProductChange are structurally the same shape (a rate plus a bounded
// skew), and PromoRule is the same shape with maxSkew fixed at
// Duration.ZERO.
public class SkewedEventStream<T> {
    private final Function<Instant, T> factory;
    private final Function<T, String> keyExtractor;
    private final Function<T, byte[]> serializer;
    private final String topic;
    private final Duration maxSkew;
    private final SkewedPublisher publisher;
    private final Random random;
    private final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();

    public SkewedEventStream(
            Function<Instant, T> factory,
            Function<T, String> keyExtractor,
            Function<T, byte[]> serializer,
            String topic,
            Duration maxSkew,
            SkewedPublisher publisher,
            Random random
    ) {
        this.factory = factory;
        this.keyExtractor = keyExtractor;
        this.serializer = serializer;
        this.topic = topic;
        this.maxSkew = maxSkew;
        this.publisher = publisher;
        this.random = random;
    }

    public void start(double eventsPerSecond) {
        long periodMillis = Math.max(1, Math.round(1000.0 / eventsPerSecond));
        // firing tick() on that fixed schedule for as long as the program runs
        ticker.scheduleAtFixedRate(this::tick, 0, periodMillis, TimeUnit.MILLISECONDS);
    }

    private void tick() {
        // Instant.now() at each tick is already correctly ordered, since
        // ticks fire sequentially on this stream's own single-threaded
        // scheduler. No separate simulated clock needed to keep eventTime
        // honest; the publish delay below is the only thing that injects
        // skew, exactly the "delayed publish" mechanism from the design.
        Instant eventTime = Instant.now();
        T event = factory.apply(eventTime);
        Duration delay = maxSkew.isZero()
                ? Duration.ZERO
                : Duration.ofMillis(random.nextLong(maxSkew.toMillis() + 1));
        publisher.publish(topic, keyExtractor.apply(event), serializer.apply(event), delay);
    }

    public void stop() {
        ticker.shutdown();
    }
}
