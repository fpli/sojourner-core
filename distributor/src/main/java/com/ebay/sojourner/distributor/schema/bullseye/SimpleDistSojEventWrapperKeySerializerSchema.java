package com.ebay.sojourner.distributor.schema.bullseye;

import com.ebay.sojourner.common.model.SimpleDistSojEventWrapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class SimpleDistSojEventWrapperKeySerializerSchema implements SerializationSchema<SimpleDistSojEventWrapper> {

    @Override
    public byte[] serialize(SimpleDistSojEventWrapper element) {
        return element.getKey();
    }
}
