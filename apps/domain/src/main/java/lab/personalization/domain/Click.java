package lab.personalization.domain;

import java.time.Instant;

public record Click(String shopperId, String productId, Instant eventTime, ActionType actionType) {}
