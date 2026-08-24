package lab.personalization.pipeline;

import java.time.Duration;

record PipelineConfig(
        String bootstrapServers,
        String consumerGroup,
        String inputTopic,
        Duration watermarkBound,
        Duration sessionGap,
        Duration watermarkIdleness,
        boolean startFromEarliest
) {
    static PipelineConfig parse(String[] args) {
        String bootstrapServers = "localhost:30016";
        String consumerGroup = "personalization-phase-3";
        String inputTopic = "clickstream";
        Duration watermarkBound = Duration.ofSeconds(5);
        Duration sessionGap = Duration.ofSeconds(6);
        Duration watermarkIdleness = Duration.ofSeconds(5);
        boolean startFromEarliest = true;

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
                case "watermark-bound-seconds" -> watermarkBound = Duration.ofSeconds(Long.parseLong(value));
                case "session-gap-seconds" -> sessionGap = Duration.ofSeconds(Long.parseLong(value));
                case "watermark-idleness-seconds" -> watermarkIdleness = Duration.ofSeconds(Long.parseLong(value));
                case "start-from-earliest" -> startFromEarliest = parseBoolean(value);
                default -> throw new IllegalArgumentException("Unknown option: --" + key);
            }
        }

        return new PipelineConfig(bootstrapServers, consumerGroup, inputTopic,
                watermarkBound, sessionGap, watermarkIdleness, startFromEarliest);
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
