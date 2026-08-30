package lab.personalization.pipeline.operator;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import lab.personalization.domain.Click;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;
import lab.personalization.domain.SessionSignal;

public class SessionAggregator 
    extends ProcessWindowFunction<Click, SessionSignal, String, TimeWindow> {

    public static final OutputTag<Click> LATE_CLICKS =
            new OutputTag<>("late-clicks", TypeInformation.of(Click.class)) {};

    @Override
    public void process(String shopperId, Context ctx, Iterable<Click> elements, Collector<SessionSignal> out) throws Exception {
        int clicks = 0;
        Map<String, Integer> productClickCounts = new HashMap<>();

        for(Click c : elements) {
            productClickCounts.merge(c.productId(), 1, Integer::sum);
            clicks++;
        }

        String topProductId = null;
        int topCount = 0;
        for(Map.Entry<String, Integer> entry : productClickCounts.entrySet()) {
            String productId = entry.getKey();
            int count = entry.getValue();

            boolean higherCount = count > topCount;
            // Tiebreaker: if two product click counts are the same, choose the smaller productId
            boolean tieBreakerEarlier = count == topCount && productId.compareTo(topProductId) < 0;

            if(higherCount || tieBreakerEarlier) {
                topCount = count;
                topProductId = productId;
            }
        }

        out.collect(new SessionSignal(
            shopperId, 
            Instant.ofEpochMilli(ctx.window().getStart()), 
            Instant.ofEpochMilli(ctx.window().getEnd()), clicks, topProductId));
    }
}