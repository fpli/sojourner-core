package com.ebay.sojourner.integration.schema;

import com.ebay.sojourner.common.model.SojSession;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.avro.RheosEventDeserializer;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroDeserializerHelper;
import io.ebay.rheos.schema.event.RheosEvent;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;


@Slf4j
public class SojSessionRawSojSessionDeserializationSchema implements
    KafkaDeserializationSchema<SojSession> {

  private final String registryUrl;
  private transient SchemaRegistryAwareAvroDeserializerHelper<SojSession> deserializerHelper;
  private transient GenericRecordDomainDataDecoder genericRecordDomainDataDecoder;
  private transient RheosEventDeserializer rheosEventDeserializer;


  public SojSessionRawSojSessionDeserializationSchema(String registryUrl) {
    this.registryUrl = registryUrl;
  }


  @Override
  public void open(InitializationContext context) throws Exception {
    KafkaDeserializationSchema.super.open(context);
    Map<String, Object> config = new HashMap<>();
    config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, this.registryUrl);
    this.deserializerHelper = new SchemaRegistryAwareAvroDeserializerHelper<>(config, SojSession.class);
    this.genericRecordDomainDataDecoder = new GenericRecordDomainDataDecoder(config);
    this.rheosEventDeserializer = new RheosEventDeserializer();
  }

  @Override
  public boolean isEndOfStream(SojSession nextElement) {
    return false;
  }

  @Override
  public SojSession deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

    RheosEvent rheosEvent = rheosEventDeserializer.deserialize(null, record.value());

    // test if genericRecordDomainDataDecoder is working
    GenericRecord genericRecord = genericRecordDomainDataDecoder.decode(rheosEvent);

    // test if SchemaRegistryAwareAvroDeserializerHelper is working
    int schemaId = rheosEvent.getSchemaId();
    SojSession sojSession = deserializerHelper.deserializeData(schemaId, rheosEvent.toBytes());

    log.info("schemaId: {}, SojSession: {}", rheosEvent.getSchemaId(), sojSession);

    return sojSession;
  }

  @Override
  public TypeInformation<SojSession> getProducedType() {
    return TypeInformation.of(SojSession.class);
  }
}
