package lab.personalization.generator;

import java.time.Duration;

// Parsed from --key=value CLI args, sensible defaults for everything so it
// runs with zero args for local development. No external CLI library: the
// design doc left the parsing mechanism unspecified, and a handful of flat
// options doesn't need more than this.
//
// Package-private on purpose, not public: nothing outside this package
// needs it. Its canonical constructor is package-private too, matching its
// own access level, same rule worked through earlier this session.
record GeneratorConfig(
        String bootstrapServers,
        double clickEventsPerSecond,
        Duration clickMaxSkew,
        double productChangeEventsPerSecond,
        Duration productChangeMaxSkew,
        Duration promoRuleInterval
) {
    static GeneratorConfig parse(String[] args) {
        String bootstrapServers = "localhost:30016";
        double clickRate = 5.0;
        Duration clickSkew = Duration.ofSeconds(2);
        double productChangeRate = 1.0;
        Duration productChangeSkew = Duration.ofSeconds(2);
        Duration promoRuleInterval = Duration.ofSeconds(30);

        for (String arg : args) {
            String[] parts = arg.replaceFirst("^--", "").split("=", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Expected --key=value, got: " + arg);
            }
            String key = parts[0];
            String value = parts[1];
            switch (key) {
                case "bootstrap-servers" -> bootstrapServers = value;
                case "click-rate" -> clickRate = Double.parseDouble(value);
                case "click-max-skew-seconds" -> clickSkew = Duration.ofSeconds(Long.parseLong(value));
                case "product-change-rate" -> productChangeRate = Double.parseDouble(value);
                case "product-change-max-skew-seconds" -> productChangeSkew = Duration.ofSeconds(Long.parseLong(value));
                case "promo-rule-interval-seconds" -> promoRuleInterval = Duration.ofSeconds(Long.parseLong(value));
                default -> throw new IllegalArgumentException("Unknown option: --" + key);
            }
        }

        return new GeneratorConfig(bootstrapServers, clickRate, clickSkew,
                productChangeRate, productChangeSkew, promoRuleInterval);
    }
}
