package com.ebay.sojourner.distributor.schema;

import com.ebay.sojourner.common.model.RawSojSessionWrapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RawSojSessionWrapperDeserializationSchema implements
    KafkaDeserializationSchema<RawSojSessionWrapper> {

  @Override
  public boolean isEndOfStream(RawSojSessionWrapper nextElement) {
    return false;
  }

  @Override
  public RawSojSessionWrapper deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
    return new RawSojSessionWrapper(record.key(), record.value());
  }

  @Override
  public TypeInformation<RawSojSessionWrapper> getProducedType() {
    return TypeInformation.of(RawSojSessionWrapper.class);
  }
}
