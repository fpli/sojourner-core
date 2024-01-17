package com.ebay.sojourner.flink.connector.kafka.schema.serialize;

import com.ebay.sojourner.common.model.SojSession;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.charset.StandardCharsets;

public class SojSessionKeySerializerSchema implements SerializationSchema<SojSession> {
    @Override
    public byte[] serialize(SojSession element) {

        return element.getGuid().getBytes(StandardCharsets.UTF_8);
    }
}
