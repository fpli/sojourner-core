package com.ebay.sojourner.flink.connector.kafka.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class PassThroughDeserializationSchema implements KafkaDeserializationSchema<byte[]> {

    @Override
    public boolean isEndOfStream(byte[] nextElement) {
        return false;
    }

    @Override
    public byte[] deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return record.value();
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return TypeInformation.of(byte[].class);
    }
}
