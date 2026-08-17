package lab.personalization.generator.domain;

// Deliberately simple: the discount condition itself (e.g. "within 2
// seconds of a price drop") lives in the Flink job's own logic in Phase 4,
// not in this record. See the design doc for what was considered instead.
public record PromoRule(String ruleId, String description, double discountPercent) {}
