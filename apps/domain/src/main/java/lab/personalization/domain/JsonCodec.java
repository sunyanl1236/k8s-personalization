package lab.personalization.domain;

import java.nio.charset.StandardCharsets;

// Hand-written, not a library: the schemas are small and flat enough that
// reflection-based JSON mapping isn't worth the dependency (and Java
// records have their own caveats there). ProductChange's "type" field is
// exactly what a sealed interface doesn't carry across serialization on
// its own, see the design doc.
//
// Instant.toString() already returns ISO-8601 UTC, e.g.
// "2026-08-16T10:00:01.123456Z", safe to embed directly, no separate
// formatter needed.
public final class JsonCodec {

    public static byte[] toJson(Click click) {
        String json = """
                {"shopperId":"%s","productId":"%s","eventTime":"%s","actionType":"%s"}\
                """.formatted(click.shopperId(), click.productId(), click.eventTime(), click.actionType());
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] toJson(ProductChange change) {
        String json = switch (change) {
            case PriceChange p -> """
                    {"type":"PRICE","productId":"%s","eventTime":"%s","newPrice":%s}\
                    """.formatted(p.productId(), p.eventTime(), p.newPrice());
            case StockChange s -> """
                    {"type":"STOCK","productId":"%s","eventTime":"%s","newStock":%d}\
                    """.formatted(s.productId(), s.eventTime(), s.newStock());
        };
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] toJson(PromoRule rule) {
        String json = """
                {"ruleId":"%s","description":"%s","discountPercent":%s}\
                """.formatted(rule.ruleId(), rule.description(), rule.discountPercent());
        return json.getBytes(StandardCharsets.UTF_8);
    }

    private JsonCodec() {}
}
