# Generator event-production design

Date: 2026-08-16
Status: approved (design), not yet implemented
Derives from: [domain schemas design](2026-08-16-domain-schemas-design.md),
Phase 2 of
[the implementation plan](../plans/2026-08-10-implementation-phases.md).

## Goal

Design the generator's actual event-production logic: producing `Click`,
`PriceChange`/`StockChange`, and `PromoRule` events and publishing them to
`clickstream`, `product-change`, and `promo-rule`, satisfying Phase 2's
done-when criterion: "the generator sustains a configurable event rate with
a configurable maximum skew, and a consumer confirms both."

## Decisions

### Plain Java producer, not a Flink program

The generator doesn't process a stream, it produces one: no windowing, no
keyed state, no checkpointing. Built as a plain Java program using
`org.apache.kafka:kafka-clients` directly, the same conceptual role `kcat`
played in Phase 1's external-listener drill. Consequence for
`generator/build.gradle`: `flink-streaming-java`/`flink-clients` are not
needed for the generator and should be removed; `kafka-clients` added in
their place. Considered and rejected: a minimal Flink DataStream job (custom
`SourceFunction` plus a `KafkaSink`), which would justify keeping the Flink
dependencies but pulls in parallelism, checkpointing, and restart-strategy
machinery for what is fundamentally a producer loop.

### Delayed publish, not batch-and-shuffle, for skew injection

Each event's `eventTime` is computed in strict, correct, increasing order,
one per tick of the configured rate. Publishing is what gets delayed: for
each event, a delay is chosen uniformly at random between zero and the
stream's configured maximum skew, and the actual `KafkaProducer.send()`
happens that far in the future on a real clock. `eventTime` stays honest;
the moment Kafka actually receives the message lags behind by a bounded,
known amount. This produces real arrival-time skew against a live topic,
observable by a real consumer, which is what Phase 3's watermark and
lateness logic needs to be tested against.

Considered and rejected: generating a full batch of correctly-timestamped
events in memory, then shuffling publish order within a bounded window and
sending the batch immediately. Faster to run, no real waiting, but a
one-shot batch rather than something that "sustains" a rate the way the
phase plan's done-when criterion describes, and less faithful to a live
producer with real network jitter.

### JSON, not Avro

JSON was chosen over Avro despite Avro (and Protobuf) being the more common
production choice at scale, since Avro's schema enforcement requires
running a Schema Registry, infrastructure this project's spec doesn't
otherwise call for anywhere. JSON stays directly inspectable with the same
`kcat -C` command already used in Phase 1's drill, no extra tooling needed
to verify what's actually flowing through a topic.

Consequence: `PriceChange` and `StockChange` share the `product-change`
topic, and neither JSON nor a sealed interface itself carries that
distinction across serialization. Every `ProductChange` message includes an
explicit `"type"` field, `"PRICE"` or `"STOCK"`, so a reader can tell them
apart without out-of-band knowledge.

### `PromoRule` gets its own cadence, not the shared rate/skew

`Click` and `ProductChange` are continuous, high-frequency streams;
`PromoRule` is different in kind per `CONTEXT.md`, broadcast to every
subtask and only changing occasionally. It's configured with its own,
separate, much slower interval, and publishes immediately with no injected
delay: broadcast state has no watermark or window gating it, so skew isn't
a meaningful concept for this stream the way it is for the other two.

## Architecture

One reusable driver, instantiated three times rather than three
hand-written loops, since `Click` and `ProductChange` are structurally the
same shape (a continuous stream needing a rate and a bounded skew) even
though their content differs, and `PromoRule` is the same shape with skew
fixed at zero.

```java
class SkewedEventStream<T> {
    // rate: how often a new logical event gets generated
    // maxSkew: zero for none, or a Duration bound for delayed publish
    // factory: produces the next T given the current logical time
    // topic: which Kafka topic it publishes to
}
```

Three instances:

- `SkewedEventStream<Click>`: configurable rate, configurable skew.
- `SkewedEventStream<ProductChange>`: its own configurable rate, its own
  configurable skew, independent of `Click`'s, since Phase 4's interval join
  needs its own watermark on this stream too.
- `SkewedEventStream<PromoRule>`: its own, much slower configurable
  interval, skew fixed at zero.

## Components

- **Event factories** (`ClickFactory`, `ProductChangeFactory`,
  `PromoRuleFactory`): pure, stateless-ish generation logic over a small
  fixed pool of shopper/product IDs. No Kafka, no scheduling involved, so
  each is testable on its own.
- **`JsonCodec`**: serializes a `Click`/`ProductChange`/`PromoRule` to JSON
  bytes, including the `"type"` discriminator for `ProductChange`.
- **`SkewedPublisher`**: the only component touching Kafka or real
  wall-clock delay. Given serialized bytes, a topic, and a delay (possibly
  zero), schedules the actual `KafkaProducer.send()` that far in the
  future.
- **`SkewedEventStream<T>`**: wires one factory, the codec, and the
  publisher together into one running, rate-driven stream.
- **`Generator`**: the main class. Reads CLI config, constructs the three
  `SkewedEventStream` instances, starts them, runs until stopped.

## Configuration

CLI args, not a config file or environment variables, since this is a
single runnable tool, not a long-lived service: Kafka bootstrap servers,
rate and max-skew for `Click`, rate and max-skew for `ProductChange`
(independently configurable from `Click`'s), and the interval for
`PromoRule`.

## Verifying the done-when criterion

Run the generator with known rate and skew values, then verify
independently with `kcat -C` against the real topics, the same standard
Phase 1's external-listener drill already established: confirm the observed
throughput matches the configured rate, and confirm no event's Kafka
arrival lags its own `eventTime` by more than the configured skew bound.
Not built into the generator itself; a separate verification step, matching
this project's own "don't trust it, prove it with a real client" pattern.

## Out of scope for this design

- The actual CLI argument parsing library/mechanism.
- Unit test structure for the factories and codec.
- What happens to `PromoRule`'s content over time (whether rules actually
  change meaningfully across successive generations, or just repeat);
  that's the event-factory's own generation logic, not this design's
  architecture.
