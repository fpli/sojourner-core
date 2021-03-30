package com.ebay.sojourner.distributor.schema;


import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.google.common.collect.Lists;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.jetbrains.annotations.Nullable;

public class RawSojEventWrapperSerializationSchema
    implements KafkaSerializationSchema<RawSojEventWrapper> {

  private static final String SCHEMA_VERSION = "2";
  private static final List<Header> headers = Lists.newArrayList(
      new RecordHeader("schemaVersion", SCHEMA_VERSION.getBytes(StandardCharsets.UTF_8)));

  @Override
  public ProducerRecord<byte[], byte[]> serialize(RawSojEventWrapper element,
                                                  @Nullable Long timestamp) {

    return new ProducerRecord<>(element.getTopic(), null,
                                element.getGuid().getBytes(),
                                element.getPayload(), headers);
  }
}
