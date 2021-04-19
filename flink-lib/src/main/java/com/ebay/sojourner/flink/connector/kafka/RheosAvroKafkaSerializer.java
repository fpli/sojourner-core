package com.ebay.sojourner.flink.connector.kafka;

import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroSerializerHelper;
import io.ebay.rheos.schema.event.RheosEvent;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.common.errors.SerializationException;

@Slf4j
public class RheosAvroKafkaSerializer<T extends SpecificRecord> implements KafkaSerializer<T> {

  private static final String FIELD_DELIM = ",";
  private final RheosKafkaProducerConfig rheosKafkaConfig;
  private final SchemaRegistryAwareAvroSerializerHelper<T> serializerHelper;
  private DatumWriter<RheosEvent> writer; // init when using
  private final int schemaId;

  public RheosAvroKafkaSerializer(RheosKafkaProducerConfig rheosKafkaConfig, Class<T> clazz) {
    this.rheosKafkaConfig = rheosKafkaConfig;
    this.serializerHelper =
        new SchemaRegistryAwareAvroSerializerHelper<>(rheosKafkaConfig.toConfigMap(), clazz);
    this.schemaId = serializerHelper.getSchemaId(rheosKafkaConfig.getSchemaSubject());
  }

  @Override
  public byte[] encodeKey(T data, List<String> keyFields) {
    if (data == null || CollectionUtils.isEmpty(keyFields)) {
      return null;
    } else {
      Field field = null;
      List<String> valueList = new ArrayList<>();
      for (String keyName : keyFields) {
        try {
          field = data.getClass().getDeclaredField(keyName);
          field.setAccessible(true);
          Object value = field.get(data);
          valueList.add(String.valueOf(value));
        } catch (Exception e) {
          log.error("Get field[{}] value error", keyName, e);
          // throw new RuntimeException(e);
        }
      }
      return String.join(FIELD_DELIM, valueList).getBytes(StandardCharsets.UTF_8);
    }
  }

  @Override
  public byte[] encodeValue(T data) {
    Schema schema = serializerHelper.getSchema(this.schemaId);
    if (writer == null) {
      writer = new GenericDatumWriter<>(schema);
    }

    GenericRecord record = (GenericRecord) GenericData.get().deepCopy(schema, data);

    RheosEvent rheosEvent = new RheosEvent(record);

    rheosEvent.setEventCreateTimestamp(System.currentTimeMillis());
    rheosEvent.setEventSentTimestamp(System.currentTimeMillis());
    rheosEvent.setSchemaId(this.schemaId);
    rheosEvent.setProducerId(rheosKafkaConfig.getProducerId());

    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] serializedValue = null;
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      writer.write(rheosEvent, encoder);
      encoder.flush();
      serializedValue = out.toByteArray();
      out.close();
      return serializedValue;
    } catch (Exception e) {
      throw new SerializationException("Error serializing Avro schema for schema " +
                                       schema.getName(), e);
    }
  }
}