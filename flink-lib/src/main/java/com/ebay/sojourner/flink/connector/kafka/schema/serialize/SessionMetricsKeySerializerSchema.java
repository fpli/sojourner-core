package com.ebay.sojourner.flink.connector.kafka.schema.serialize;

import com.ebay.sojourner.common.model.SessionMetrics;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.charset.StandardCharsets;

public class SessionMetricsKeySerializerSchema implements SerializationSchema<SessionMetrics> {
    @Override
    public byte[] serialize(SessionMetrics element) {

        return element.getGuid().getBytes(StandardCharsets.UTF_8);
    }
}
