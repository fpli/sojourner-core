package com.ebay.sojourner.flink.function.map;

import com.ebay.sojourner.common.model.SojSession;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroDeserializerHelper;
import io.ebay.rheos.schema.event.RheosEvent;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

@Slf4j
public class RheosEventToSojSessionMapFunction extends RichMapFunction<RheosEvent, SojSession> {

  private final String rheosRegistryUrl;
  private transient SchemaRegistryAwareAvroDeserializerHelper<SojSession> deserializerHelper;
  private transient GenericRecordDomainDataDecoder genericRecordDomainDataDecoder;

  public RheosEventToSojSessionMapFunction(String rheosRegistryUrl) {
    this.rheosRegistryUrl = rheosRegistryUrl;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, this.rheosRegistryUrl);
    this.deserializerHelper = new SchemaRegistryAwareAvroDeserializerHelper<>(config, SojSession.class);
    this.genericRecordDomainDataDecoder = new GenericRecordDomainDataDecoder(config);
  }

  @Override
  public SojSession map(RheosEvent rheosEvent) throws Exception {
    // covert RheosEvent to avro GenericRecord
    GenericRecord genericRecord = genericRecordDomainDataDecoder.decode(rheosEvent);

    // test if SchemaRegistryAwareAvroDeserializerHelper is working
    int schemaId = rheosEvent.getSchemaId();
    SojSession sojSession = deserializerHelper.deserializeData(schemaId, rheosEvent.toBytes());

    return null;
  }


}
