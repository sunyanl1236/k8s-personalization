package lab.personalization.domain;

import java.time.Instant;

public record ShopperSignal(
        String shopperId,
        SignalKind kind,
        Instant eventTime,
        String productId) {

    public static ShopperSignal browsingSession(SessionSignal session) {
        return new ShopperSignal(session.shopperId(), SignalKind.BROWSING_SESSION,
                session.windowEnd(), session.topProductId());
    }

    public static ShopperSignal cartAbandonment(String shopperId, Instant eventTime, String productId) {
        return new ShopperSignal(shopperId, SignalKind.CART_ABANDONMENT, eventTime, productId);
    }
}
