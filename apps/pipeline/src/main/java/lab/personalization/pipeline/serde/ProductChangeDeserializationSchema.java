package lab.personalization.pipeline.serde;

import lab.personalization.domain.JsonCodec;
import lab.personalization.domain.ProductChange;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class ProductChangeDeserializationSchema implements DeserializationSchema<ProductChange> {

    @Override
    public ProductChange deserialize(byte[] message) {
        return JsonCodec.productChangeFromJson(message);
    }

    @Override
    public boolean isEndOfStream(ProductChange nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ProductChange> getProducedType() {
        return TypeInformation.of(ProductChange.class);
    }
}
