package lab.personalization.pipeline.serde;

import lab.personalization.domain.JsonCodec;
import lab.personalization.domain.PromoRule;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class PromoRuleDeserializationSchema implements DeserializationSchema<PromoRule> {

    @Override
    public PromoRule deserialize(byte[] message) {
        return JsonCodec.promoRuleFromJson(message);
    }

    @Override
    public boolean isEndOfStream(PromoRule nextElement) {
        return false;
    }

    @Override
    public TypeInformation<PromoRule> getProducedType() {
        return TypeInformation.of(PromoRule.class);
    }
}
