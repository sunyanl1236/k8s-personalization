package lab.personalization.pipeline.service;

import lab.personalization.domain.RecommendationRequest;

import java.util.concurrent.CompletableFuture;

public interface RecommendationClient extends AutoCloseable {

    CompletableFuture<String> suggest(RecommendationRequest request);

    @Override
    void close();
}
