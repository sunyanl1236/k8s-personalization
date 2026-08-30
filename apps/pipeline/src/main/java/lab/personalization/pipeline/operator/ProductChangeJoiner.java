package lab.personalization.pipeline.operator;

import lab.personalization.domain.Click;
import lab.personalization.domain.EnrichedClick;
import lab.personalization.domain.ProductChange;

import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class ProductChangeJoiner extends ProcessJoinFunction<Click, ProductChange, EnrichedClick> {

    @Override
    public void processElement(Click click, ProductChange change, Context ctx,
                               Collector<EnrichedClick> out) {
        out.collect(new EnrichedClick(
                click.shopperId(),
                click.productId(),
                click.eventTime(),
                change.price(),
                change.previousPrice(),
                change.stock(),
                change.eventTime()));
    }
}
