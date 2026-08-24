package lab.personalization.domain;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    // The reverse direction, added in Phase 3: the pipeline reads what the
    // generator writes. Regex rather than index arithmetic, so field order and
    // incidental whitespace do not matter. That tolerance is not decoration:
    // Drill B injects a hand-typed Click with kcat, and a parser that depended
    // on exact byte layout would reject it for the wrong reason and make the
    // Drill look like a lateness failure.
    private static final Pattern SHOPPER_ID = stringField("shopperId");
    private static final Pattern PRODUCT_ID = stringField("productId");
    private static final Pattern EVENT_TIME = stringField("eventTime");
    private static final Pattern ACTION_TYPE = stringField("actionType");

    private static Pattern stringField(String name) {
        return Pattern.compile("\"" + name + "\"\\s*:\\s*\"([^\"]*)\"");
    }

    public static Click fromJson(byte[] bytes) {
        String json = new String(bytes, StandardCharsets.UTF_8);
        return new Click(
                required(json, SHOPPER_ID, "shopperId"),
                required(json, PRODUCT_ID, "productId"),
                Instant.parse(required(json, EVENT_TIME, "eventTime")),
                ActionType.valueOf(required(json, ACTION_TYPE, "actionType")));
    }

    private static String required(String json, Pattern pattern, String name) {
        Matcher m = pattern.matcher(json);
        if (!m.find()) {
            throw new IllegalArgumentException(
                    "Missing string field '" + name + "' in: " + json);
        }
        return m.group(1);
    }

    private JsonCodec() {}
}
