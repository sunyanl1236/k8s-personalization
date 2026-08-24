package lab.personalization.pipeline;

import lab.personalization.domain.Click;
import lab.personalization.domain.JsonCodec;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

// An adapter, not a parser. All the parsing lives in JsonCodec over in
// :domain, which knows nothing about Flink. This class exists only so Flink
// has something implementing its interface to call. That split is what keeps
// :domain dependency-free, which in turn is what keeps the records valid Flink
// POJO types rather than Kryo generic types.
public class ClickDeserializationSchema implements DeserializationSchema<Click> {

    @Override
    public Click deserialize(byte[] message) {
        // Fail fast. The alternative, catching and returning null to skip the
        // record, is defensible in production where one malformed message
        // should not stop a pipeline. It is the wrong default here for two
        // reasons. The only producer is this project's own generator, so a
        // malformed record means JsonCodec's two directions have drifted
        // apart, which is exactly the bug the shared :domain module exists to
        // prevent and exactly the bug you want loud. And a silent skip is
        // indistinguishable, from the outside, from a Late Click being routed
        // to the side output, which would make Drill B unreadable.
        return JsonCodec.fromJson(message);
    }

    // Deliberately no @Override. This method exists in the 1.x line and its
    // presence in 2.2 was not verified. Without the annotation the class
    // compiles whether the interface still declares it (this implements it) or
    // no longer does (this is a harmless unused method). With the annotation,
    // being wrong is a compile error for no benefit.
    public boolean isEndOfStream(Click nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Click> getProducedType() {
        // The method people forget. Java erases the generic parameter, so
        // without this Flink cannot recover the stream's element type, falls
        // back to a generic type, and pipeline.generic-types: false rejects
        // the job at graph construction.
        return TypeInformation.of(Click.class);
    }
}
