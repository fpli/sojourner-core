package com.ebay.sojourner.flink.connector.kafka.schema;

import com.ebay.sojourner.common.model.SojSession;
import java.io.IOException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SojSessionRheosDeserialization implements KafkaRecordDeserializationSchema<SojSession> {

  private final String rheosRegistryUrl;

  public SojSessionRheosDeserialization(String rheosRegistryUrl) {
    this.rheosRegistryUrl = rheosRegistryUrl;
  }

  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<SojSession> out) throws IOException {
    // to be implemented
  }

  @Override
  public TypeInformation<SojSession> getProducedType() {
    return null;
  }
}
