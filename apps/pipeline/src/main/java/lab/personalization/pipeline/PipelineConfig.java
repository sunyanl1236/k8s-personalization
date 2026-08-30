package lab.personalization.pipeline;

import java.time.Duration;
import java.util.List;

record PipelineConfig(
        String bootstrapServers,
        String consumerGroup,
        String inputTopic,
        String productChangeTopic,
        String promoRuleTopic,
        String flinkConfDir,
        Duration watermarkBound,
        Duration sessionGap,
        Duration joinLowerBound,
        Duration joinUpperBound,
        Duration cepWithin,
        Duration abandonmentTtl,
        List<String> recommendationCatalogue,
        Duration recommendationLatency,
        Duration recommendationTimeout,
        int recommendationCapacity,
        boolean debugPrints,
        Duration cooldown,
        Duration watermarkIdleness,
        boolean startFromEarliest,
        String outputTopic,
        String transactionalIdPrefix,
        Duration transactionTimeout,
        boolean bounded,
        String restoreFrom
) {
    static PipelineConfig parse(String[] args) {
        String bootstrapServers = "localhost:30016";
        String consumerGroup = "personalization-phase-3";
        String inputTopic = "clickstream";
        String productChangeTopic = "product-change";
        String promoRuleTopic = "promo-rule";
        String flinkConfDir = "conf";
        Duration watermarkBound = Duration.ofSeconds(5);
        Duration sessionGap = Duration.ofSeconds(6);
        Duration joinLowerBound = Duration.ofSeconds(-2);
        Duration joinUpperBound = Duration.ofSeconds(2);
        Duration cepWithin = Duration.ofSeconds(30);
        Duration abandonmentTtl = Duration.ofSeconds(60);
        List<String> recommendationCatalogue =
                List.of("P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10");
        Duration recommendationLatency = Duration.ofMillis(40);
        Duration recommendationTimeout = Duration.ofMillis(1000);
        int recommendationCapacity = 100;
        boolean debugPrints = true;
        Duration cooldown = Duration.ofSeconds(60);
        Duration watermarkIdleness = Duration.ofSeconds(5);
        boolean startFromEarliest = true;
        String outputTopic = "recommendation";
        String transactionalIdPrefix = "personalization-phase-3";
        Duration transactionTimeout = Duration.ofMillis(300_000);
        boolean bounded = false;
        String restoreFrom = null;

        for (String arg : args) {
            String[] parts = arg.replaceFirst("^--", "").split("=", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Expected --key=value, got: " + arg);
            }
            String key = parts[0];
            String value = parts[1];
            switch (key) {
                case "bootstrap-servers" -> bootstrapServers = value;
                case "consumer-group" -> consumerGroup = value;
                case "input-topic" -> inputTopic = value;
                case "product-change-topic" -> productChangeTopic = value;
                case "promo-rule-topic" -> promoRuleTopic = value;
                case "flink-conf-dir" -> flinkConfDir = value;
                case "watermark-bound-seconds" -> watermarkBound = Duration.ofSeconds(Long.parseLong(value));
                case "session-gap-seconds" -> sessionGap = Duration.ofSeconds(Long.parseLong(value));
                case "join-lower-bound-seconds" -> joinLowerBound = Duration.ofSeconds(Long.parseLong(value));
                case "join-upper-bound-seconds" -> joinUpperBound = Duration.ofSeconds(Long.parseLong(value));
                case "cep-within-seconds" -> cepWithin = Duration.ofSeconds(Long.parseLong(value));
                case "abandonment-ttl-seconds" -> abandonmentTtl = Duration.ofSeconds(Long.parseLong(value));
                case "recommendation-catalogue" -> recommendationCatalogue = List.of(value.split(","));
                case "recommendation-latency-ms" -> recommendationLatency = Duration.ofMillis(Long.parseLong(value));
                case "recommendation-timeout-ms" -> recommendationTimeout = Duration.ofMillis(Long.parseLong(value));
                case "recommendation-capacity" -> recommendationCapacity = Integer.parseInt(value);
                case "debug-prints" -> debugPrints = parseBoolean(value);
                case "cooldown-seconds" -> cooldown = Duration.ofSeconds(Long.parseLong(value));
                case "watermark-idleness-seconds" -> watermarkIdleness = Duration.ofSeconds(Long.parseLong(value));
                case "start-from-earliest" -> startFromEarliest = parseBoolean(value);
                case "output-topic" -> outputTopic = value;
                case "transactional-id-prefix" -> transactionalIdPrefix = value;
                case "transaction-timeout-ms" -> transactionTimeout = Duration.ofMillis(Long.parseLong(value));
                case "bounded" -> bounded = parseBoolean(value);
                case "restore-from" -> restoreFrom = value;
                default -> throw new IllegalArgumentException("Unknown option: --" + key);
            }
        }

        return new PipelineConfig(bootstrapServers, consumerGroup, inputTopic, productChangeTopic, promoRuleTopic, flinkConfDir,
                watermarkBound, sessionGap, joinLowerBound, joinUpperBound, cepWithin, abandonmentTtl, recommendationCatalogue, recommendationLatency,
                recommendationTimeout, recommendationCapacity, debugPrints, cooldown, watermarkIdleness, startFromEarliest,
                outputTopic, transactionalIdPrefix, transactionTimeout, bounded, restoreFrom);
    }

    private static boolean parseBoolean(String value) {
        if (value.equalsIgnoreCase("true")) {
            return true;
        }
        if (value.equalsIgnoreCase("false")) {
            return false;
        }
        throw new IllegalArgumentException("Expected true or false, got: " + value);
    }
}
