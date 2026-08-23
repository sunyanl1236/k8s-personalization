package lab.personalization.domain;

import java.time.Instant;

public record StockChange(String productId, Instant eventTime, int newStock) implements ProductChange {}
