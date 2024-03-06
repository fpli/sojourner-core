package com.ebay.sojourner.distributor.schema;


import com.ebay.sojourner.common.model.RawSojSessionWrapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.Nullable;

@Deprecated
public class RawSojSessionWrapperSerializationSchema
    implements KafkaSerializationSchema<RawSojSessionWrapper> {

  private final String topic;

  public RawSojSessionWrapperSerializationSchema(String topic) {
    this.topic = topic;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(RawSojSessionWrapper element,
                                                  @Nullable Long timestamp) {

    return new ProducerRecord<>(topic, null,
                                element.getKey(),
                                element.getValue());
  }
}
