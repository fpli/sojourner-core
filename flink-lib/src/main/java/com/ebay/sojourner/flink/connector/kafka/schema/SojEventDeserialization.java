package com.ebay.sojourner.flink.connector.kafka.schema;

import com.ebay.sojourner.common.model.SojEvent;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Slf4j
public class SojEventDeserialization implements KafkaRecordDeserializationSchema<SojEvent> {

  private transient DatumReader<SojEvent> reader;

  @Override
  public void open(InitializationContext context) throws Exception {
    KafkaRecordDeserializationSchema.super.open(context);
    log.info("Initialize SojEvent DatumReader.");
    reader = new SpecificDatumReader<>(SojEvent.class);
  }

  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<SojEvent> out) throws IOException {
    Decoder decoder = null;
    try {
      decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
      SojEvent sojEvent = reader.read(null, decoder);
      out.collect(sojEvent);
    } catch (IOException e) {
      throw new RuntimeException("Deserialize SojSession error.", e);
    }
  }

  @Override
  public TypeInformation<SojEvent> getProducedType() {
    return TypeInformation.of(SojEvent.class);
  }
}
