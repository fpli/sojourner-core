package com.ebay.sojourner.flink.connector.kafka.schema.serialize;


import com.ebay.sojourner.common.model.RheosHeader;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class RheosHeaderKeySerializerSchema implements SerializationSchema<RheosHeader> {

    @Override
    public byte[] serialize(RheosHeader element) {
        return element.getEventId().getBytes();
    }
}
