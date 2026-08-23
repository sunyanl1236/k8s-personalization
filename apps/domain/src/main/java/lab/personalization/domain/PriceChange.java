package lab.personalization.domain;

import java.time.Instant;

public record PriceChange(String productId, Instant eventTime, double newPrice) implements ProductChange {}
