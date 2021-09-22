package com.ebay.sojourner.flink.connector.kafka.schema;

import com.ebay.sojourner.common.constant.KafkaMessageHeaders;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.flink.connector.kafka.KafkaSerializer;
import com.ebay.sojourner.flink.connector.kafka.RheosAvroKafkaSerializer;
import com.ebay.sojourner.flink.connector.kafka.RheosKafkaProducerConfig;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.jetbrains.annotations.Nullable;

public class SojEventKafkaSerializationSchema extends RheosKafkaSerializationSchema<SojEvent> {

  private final RheosKafkaProducerConfig rheosKafkaConfig;
  private transient KafkaSerializer<SojEvent> rheosKafkaSerializer;

  public SojEventKafkaSerializationSchema(RheosKafkaProducerConfig rheosKafkaConfig) {
    super(rheosKafkaConfig, SojEvent.class, "guid");
    this.rheosKafkaConfig = rheosKafkaConfig;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(SojEvent element, @Nullable Long timestamp) {
    int pageId = element.getPageId() == null ? -1 : element.getPageId();
    Header pageIdHeader = new RecordHeader(KafkaMessageHeaders.PAGE_ID,
        Ints.toByteArray(pageId));

    return new ProducerRecord<>(rheosKafkaConfig.getTopic(),
        null,
        element.getGuid().getBytes(StandardCharsets.UTF_8),
        rheosKafkaSerializer.encodeValue(element),
        Lists.newArrayList(pageIdHeader));
  }

  @Override
  public void open(InitializationContext context) throws Exception {
    this.rheosKafkaSerializer = new RheosAvroKafkaSerializer<>(rheosKafkaConfig, SojEvent.class);
  }
}
