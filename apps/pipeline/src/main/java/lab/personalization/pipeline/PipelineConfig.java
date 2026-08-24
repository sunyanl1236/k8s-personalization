package lab.personalization.pipeline;

import java.time.Duration;

// Same shape as GeneratorConfig on purpose: a record, --key=value parsing,
// defaults for everything so it runs with zero args, no CLI library. Two
// programs driven the same way is worth more than either one being marginally
// nicer on its own.
//
// Package-private, like GeneratorConfig. Nothing outside this package needs it.
//
// Fields arrive as the tasks that need them land. Still to come: cooldown
// (Task 6), S3 endpoint and checkpoint settings (Task 7), output topic and
// transactional id prefix (Task 8), bounded mode and restore path (Task 9).
// Adding them early would be configuration for behaviour that does not exist.
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

        // 5s, comfortably above the generator's 2s default skew, so no Click
        // becomes a Late Click by accident.
        Duration watermarkBound = Duration.ofSeconds(5);

        // 6s, DERIVED not chosen. Catalog.SHOPPER_IDS holds 10 Shoppers and
        // the default rate is 5 Clicks/sec, so each Shopper produces 0.5
        // Clicks/sec and the mean gap between one Shopper's Clicks is 2s. A
        // Browsing Session closes only when a gap exceeds this value, with
        // probability e^(-0.5 * gap): 5% at 6s, 0.67% at 10s, and effectively
        // zero at 30s. Set it to something that merely "sounds like a browsing
        // session" and the job emits nothing at all while behaving correctly.
        Duration sessionGap = Duration.ofSeconds(6);

        // How long a source subtask may go without data before it stops
        // holding the watermark down.
        //
        // Not a tuning knob, a correctness fix. An operator's watermark is the
        // MINIMUM across its inputs, which is the only safe rule: it cannot
        // claim to have seen everything up to T while some input might still
        // deliver something older. Default parallelism here is nproc (16) and
        // clickstream has 3 partitions, so 13 source subtasks are assigned no
        // partition at all, never see a record, never emit a watermark, and
        // pin the minimum at Long.MIN_VALUE forever. No window can then fire.
        // Observed 2026-08-23: zero SessionSignals against a 2.9-million-Click
        // backlog, with no exception.
        //
        // 5s is roughly 8x the mean gap between records on one partition
        // (3 partitions at 5 Clicks/sec is ~0.6s), so a live partition is very
        // unlikely to be marked idle by accident. Too short and a merely slow
        // partition gets marked idle, and its records then arrive behind a
        // watermark that moved on without it.
        //
        // Note this is the real fix rather than pinning parallelism to 3:
        // Phase 6 changes parallelism on purpose, and a genuinely quiet
        // partition would reproduce the same stall at any parallelism.
        Duration watermarkIdleness = Duration.ofSeconds(5);

        // earliest by default, because Task 9's Drill needs both runs to read
        // the same offset range from the beginning. Set false to watch live
        // behaviour without replaying the backlog: Task 3's 75-second run
        // printed 2.9 million Clicks doing exactly that, which makes any rate
        // check unreadable. An observation setting, not a Drill setting.
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

    // Not Boolean.parseBoolean, which never throws: it returns true only for
    // "true" and false for everything else, so --start-from-earliest=yes would
    // silently mean false. You would then run Task 9's Drill expecting a full
    // backlog replay, get live offsets instead, and watch the diff fail for a
    // reason that has nothing to do with checkpointing.
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
