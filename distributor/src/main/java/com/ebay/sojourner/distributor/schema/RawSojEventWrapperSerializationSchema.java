package com.ebay.sojourner.distributor.schema;


import static java.nio.charset.StandardCharsets.UTF_8;

import com.ebay.sojourner.common.constant.KafkaMessageHeaders;
import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.google.common.collect.Lists;
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
      new RecordHeader(KafkaMessageHeaders.SCHEMA_VERSION, SCHEMA_VERSION.getBytes(UTF_8)));

  @Override
  public ProducerRecord<byte[], byte[]> serialize(RawSojEventWrapper element,
                                                  @Nullable Long timestamp) {

    return new ProducerRecord<>(element.getTopic(), null,
                                element.getKey(),
                                element.getPayload(), headers);
  }
}
