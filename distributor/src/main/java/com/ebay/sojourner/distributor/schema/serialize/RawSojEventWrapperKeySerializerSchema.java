package com.ebay.sojourner.distributor.schema.serialize;


import com.ebay.sojourner.common.model.RawSojEventWrapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class RawSojEventWrapperKeySerializerSchema implements SerializationSchema<RawSojEventWrapper> {

    @Override
    public byte[] serialize(RawSojEventWrapper element) {
        return element.getKey();
    }
}
