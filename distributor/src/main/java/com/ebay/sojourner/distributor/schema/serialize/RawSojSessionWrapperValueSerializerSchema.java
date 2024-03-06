package com.ebay.sojourner.distributor.schema.serialize;


import com.ebay.sojourner.common.model.RawSojSessionWrapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class RawSojSessionWrapperValueSerializerSchema implements SerializationSchema<RawSojSessionWrapper> {

    @Override
    public byte[] serialize(RawSojSessionWrapper element) {
        return element.getValue();
    }
}
