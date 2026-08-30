package lab.personalization.pipeline;

import lab.personalization.domain.Recommendation;
import lab.personalization.domain.RecommendationRequest;
import lab.personalization.pipeline.operator.AsyncRecommendationLookup;
import lab.personalization.pipeline.service.RecommendationClient;
import lab.personalization.pipeline.service.RecommendationClientFactory;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncRecommendationTest {

    private static final Instant T0 = Instant.parse("2026-08-29T10:00:00Z");

    @Test
    void repliesArriveInInputOrderEvenWhenTheFirstIsSlowest() throws Exception {
        List<Recommendation> out = run(
                List.of(request("P1"), request("P2"), request("P3")),
                new StaggeredClientFactory(), 5_000L);

        assertThat(out).hasSize(3);
        // the client's answer lands in productId; reason is copied from the request
        assertThat(out.stream().map(Recommendation::productId).toList())
                .containsExactly("for-P1", "for-P2", "for-P3");
        assertThat(out).allMatch(r -> r.reason().equals("price-drop"));
    }

    @Test
    void aSlowClientHitsTheTimeoutPathAndStillEmits() throws Exception {
        List<Recommendation> out = run(
                List.of(request("P1")),
                new NeverRespondsClientFactory(), 100L);

        assertThat(out).hasSize(1);
        assertThat(out.getFirst().shopperId()).isEqualTo("shopper-1");
        assertThat(out.getFirst().productId()).isEqualTo("P1");
        assertThat(out.getFirst().discountPercent()).isEqualTo(10.0);
        assertThat(out.getFirst().generatedAt()).isEqualTo(T0);
    }

    private static List<Recommendation> run(List<RecommendationRequest> requests,
                                            RecommendationClientFactory factory,
                                            long timeoutMillis) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<RecommendationRequest> in =
                env.fromData(requests, TypeInformation.of(RecommendationRequest.class));

        DataStream<Recommendation> out = AsyncDataStream.orderedWait(
                in, new AsyncRecommendationLookup(factory),
                timeoutMillis, TimeUnit.MILLISECONDS, 100);

        List<Recommendation> collected = new ArrayList<>();
        try (CloseableIterator<Recommendation> it = out.executeAndCollect()) {
            it.forEachRemaining(collected::add);
        }
        return collected;
    }

    private static RecommendationRequest request(String candidateProductId) {
        return new RecommendationRequest("shopper-1", candidateProductId, true, false,
                10.0, "price-drop", T0);
    }

    /** First request slowest, so unorderedWait would emit P3, P2, P1. */
    private static final class StaggeredClientFactory implements RecommendationClientFactory {
        @Override
        public RecommendationClient create() {
            return new StaggeredClient();
        }
    }

    private static final class StaggeredClient implements RecommendationClient {
        private static final Map<String, Long> DELAYS = Map.of("P1", 300L, "P2", 150L, "P3", 20L);
        private final ScheduledExecutorService responder =
                Executors.newScheduledThreadPool(4, r -> {
                    Thread t = new Thread(r, "staggered-mock");
                    t.setDaemon(true);
                    return t;
                });

        @Override
        public CompletableFuture<String> suggest(RecommendationRequest request) {
            CompletableFuture<String> reply = new CompletableFuture<>();
            long delay = DELAYS.getOrDefault(request.candidateProductId(), 10L);
            responder.schedule(() -> reply.complete("for-" + request.candidateProductId()),
                    delay, TimeUnit.MILLISECONDS);
            return reply;
        }

        @Override
        public void close() {
            responder.shutdownNow();
        }
    }

    private static final class NeverRespondsClientFactory implements RecommendationClientFactory {
        @Override
        public RecommendationClient create() {
            return new NeverResponds();
        }
    }

    private static final class NeverResponds implements RecommendationClient {
        @Override
        public CompletableFuture<String> suggest(RecommendationRequest request) {
            return new CompletableFuture<>();
        }

        @Override
        public void close() {
        }
    }
}
