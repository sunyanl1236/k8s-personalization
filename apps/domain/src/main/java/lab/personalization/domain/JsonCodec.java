package lab.personalization.domain;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Hand-written, not a library: the schemas are small and flat enough that
// reflection-based JSON mapping isn't worth the dependency (and Java
// records have their own caveats there).
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
        String json = """
                {"productId":"%s","eventTime":"%s","price":%s,"previousPrice":%s,"stock":%d,"previousStock":%d}\
                """.formatted(change.productId(), change.eventTime(),
                change.price(), change.previousPrice(), change.stock(), change.previousStock());
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] toJson(PromoRule rule) {
        String json = """
                {"ruleId":"%s","description":"%s","discountPercent":%s}\
                """.formatted(rule.ruleId(), rule.description(), rule.discountPercent());
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] toJson(Recommendation recommendation) {
        String json = """
                {"shopperId":"%s","productId":"%s","discountPercent":%s,"reason":"%s","generatedAt":"%s"}\
                """.formatted(recommendation.shopperId(), recommendation.productId(),
                recommendation.discountPercent(), recommendation.reason(), recommendation.generatedAt());
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
    private static final Pattern PRICE = numberField("price");
    private static final Pattern PREVIOUS_PRICE = numberField("previousPrice");
    private static final Pattern STOCK = numberField("stock");
    private static final Pattern PREVIOUS_STOCK = numberField("previousStock");
    private static final Pattern RULE_ID = stringField("ruleId");
    private static final Pattern DESCRIPTION = stringField("description");
    private static final Pattern DISCOUNT_PERCENT = numberField("discountPercent");

    private static Pattern stringField(String name) {
        return Pattern.compile("\"" + name + "\"\\s*:\\s*\"([^\"]*)\"");
    }

    private static Pattern numberField(String name) {
        return Pattern.compile("\"" + name + "\"\\s*:\\s*(-?[0-9]+(?:\\.[0-9]+)?(?:[eE][-+]?[0-9]+)?)");
    }

    public static Click clickFromJson(byte[] bytes) {
        String json = new String(bytes, StandardCharsets.UTF_8);
        return new Click(
                required(json, SHOPPER_ID, "shopperId"),
                required(json, PRODUCT_ID, "productId"),
                Instant.parse(required(json, EVENT_TIME, "eventTime")),
                ActionType.valueOf(required(json, ACTION_TYPE, "actionType")));
    }

    public static ProductChange productChangeFromJson(byte[] bytes) {
        String json = new String(bytes, StandardCharsets.UTF_8);
        return new ProductChange(
                required(json, PRODUCT_ID, "productId"),
                Instant.parse(required(json, EVENT_TIME, "eventTime")),
                Double.parseDouble(required(json, PRICE, "price")),
                Double.parseDouble(required(json, PREVIOUS_PRICE, "previousPrice")),
                Integer.parseInt(required(json, STOCK, "stock")),
                Integer.parseInt(required(json, PREVIOUS_STOCK, "previousStock")));
    }

    public static PromoRule promoRuleFromJson(byte[] bytes) {
        String json = new String(bytes, StandardCharsets.UTF_8);
        return new PromoRule(
                required(json, RULE_ID, "ruleId"),
                required(json, DESCRIPTION, "description"),
                Double.parseDouble(required(json, DISCOUNT_PERCENT, "discountPercent")));
    }

    private static String required(String json, Pattern pattern, String name) {
        Matcher m = pattern.matcher(json);
        if (!m.find()) {
            throw new IllegalArgumentException(
                    "Missing field '" + name + "' in: " + json);
        }
        return m.group(1);
    }

    private JsonCodec() {}
}
