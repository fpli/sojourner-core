package com.ebay.sojourner.flink.connector.kafka.schema;

import com.ebay.sojourner.common.constant.SojHeaders;
import com.ebay.sojourner.common.model.RawEvent;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RawEventKafkaDeserializationSchemaWrapper implements
    KafkaDeserializationSchema<RawEvent> {

  private Set<String> skewGuids = new HashSet<>();
  private final RawEventDeserializationSchema rawEventDeserializationSchema;

  public RawEventKafkaDeserializationSchemaWrapper(Set<String> skewGuids,
      RawEventDeserializationSchema rawEventDeserializationSchema) {
    this.skewGuids = skewGuids;
    this.rawEventDeserializationSchema = rawEventDeserializationSchema;
  }

  @Override
  public boolean isEndOfStream(RawEvent nextElement) {
    return false;
  }

  @Override
  public RawEvent deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

    if (record.key() != null && skewGuids.contains(new String(record.key()))) {
      return null;
    } else {
      Long produceTimestamp = record.timestamp();
      RawEvent rawEvent = rawEventDeserializationSchema.deserialize(record.value());
      HashMap<String, Long> timestamps = new HashMap<>();
      timestamps.put(SojHeaders.PATHFINDER_PRODUCER_TIMESTAMP, produceTimestamp);
      rawEvent.setTimestamps(timestamps);
      return rawEvent;
    }
  }

  @Override
  public TypeInformation<RawEvent> getProducedType() {
    return TypeInformation.of(RawEvent.class);
  }
}
