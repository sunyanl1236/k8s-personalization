package lab.personalization.generator.domain;

import java.time.Instant;

// One Kafka topic, product-change, carries both variants on one stream.
// Whatever (de)serializes this topic has to distinguish PriceChange from
// StockChange on the wire, not assume one fixed type. Not solved here, see
// docs/superpowers/specs/2026-08-16-domain-schemas-design.md.
//
// productId()/eventTime() declared here, not repeated per variant: both
// PriceChange and StockChange already have record components with these
// exact names and types, so their generated accessors satisfy this
// interface automatically, no extra code needed in either record.
public sealed interface ProductChange permits PriceChange, StockChange {
    String productId();
    Instant eventTime();
}
