package lab.personalization.pipeline.operator;

import lab.personalization.domain.Recommendation;
import lab.personalization.domain.RecommendationRequest;
import lab.personalization.pipeline.service.RecommendationClient;
import lab.personalization.pipeline.service.RecommendationClientFactory;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;

public class AsyncRecommendationLookup
        extends RichAsyncFunction<RecommendationRequest, Recommendation> {

    private final RecommendationClientFactory factory;

    private transient RecommendationClient client;

    public AsyncRecommendationLookup(RecommendationClientFactory factory) {
        this.factory = factory;
    }

    @Override
    public void open(OpenContext openContext) {
        client = factory.create();
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void asyncInvoke(RecommendationRequest request, ResultFuture<Recommendation> resultFuture) {
        client.suggest(request).whenComplete((suggestion, failure) -> {
            if (failure != null) {
                resultFuture.complete(Collections.singleton(fallbackFor(request)));
            } else {
                resultFuture.complete(Collections.singleton(recommend(request, suggestion)));
            }
        });
    }

    @Override
    public void timeout(RecommendationRequest request, ResultFuture<Recommendation> resultFuture) {
        resultFuture.complete(Collections.singleton(fallbackFor(request)));
    }

    private static Recommendation fallbackFor(RecommendationRequest request) {
        return recommend(request, request.candidateProductId());
    }

    private static Recommendation recommend(RecommendationRequest request, String productId) {
        return new Recommendation(
                request.shopperId(),
                productId,
                request.discountPercent(),
                request.reason(),
                request.generatedAt());
    }
}
