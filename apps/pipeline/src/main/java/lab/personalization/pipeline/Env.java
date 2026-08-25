package lab.personalization.pipeline;

final class Env {

    static String require(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is not set");
        }
        return value;
    }

    private Env() {}
}
