package lab.personalization.pipeline.service;

import lab.personalization.domain.RecommendationRequest;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DeterministicMockClient implements RecommendationClient {
    // P1..P10 by default
    private final List<String> catalogue;
    private final Duration latency;
    private final ScheduledExecutorService responder;

    public DeterministicMockClient(List<String> catalogue, Duration latency) {
        this.catalogue = List.copyOf(catalogue);
        this.latency = latency;
        this.responder = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "recommendation-service-mock");
            // this thread is a helper; shut down without it
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public CompletableFuture<String> suggest(RecommendationRequest request) {
        CompletableFuture<String> reply = new CompletableFuture<>();
        responder.schedule(() -> reply.complete(answerFor(request)),
                latency.toMillis(), TimeUnit.MILLISECONDS);
        return reply;
    }

    private String answerFor(RecommendationRequest request) {
        int index = Math.floorMod(
                request.shopperId().hashCode() * 31 + request.candidateProductId().hashCode(),
                catalogue.size());
        return catalogue.get(index);
    }

    @Override
    public void close() {
        responder.shutdownNow();
    }
}
