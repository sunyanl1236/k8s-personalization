package lab.personalization.pipeline.serde;

import lab.personalization.domain.Click;
import lab.personalization.domain.JsonCodec;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class ClickDeserializationSchema implements DeserializationSchema<Click> {

    @Override
    public Click deserialize(byte[] message) {
        return JsonCodec.clickFromJson(message);
    }

    public boolean isEndOfStream(Click nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Click> getProducedType() {
        return TypeInformation.of(Click.class);
    }
}
