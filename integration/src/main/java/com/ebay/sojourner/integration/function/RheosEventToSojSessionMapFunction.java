package com.ebay.sojourner.integration.function;

import com.ebay.sojourner.common.model.SojSession;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.event.RheosEvent;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

@Slf4j
public class RheosEventToSojSessionMapFunction extends RichMapFunction<RheosEvent, SojSession> {

  private final String registryUrl;
  private transient GenericRecordDomainDataDecoder genericRecordDomainDataDecoder;

  public RheosEventToSojSessionMapFunction(String registryUrl) {
    this.registryUrl = registryUrl;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    Map<String, Object> config = new HashMap<>();
    config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, this.registryUrl);
    this.genericRecordDomainDataDecoder = new GenericRecordDomainDataDecoder(config);
  }

  @Override
  public SojSession map(RheosEvent value) throws Exception {

    GenericRecord genericRecord = genericRecordDomainDataDecoder.decode(value);
    log.info("schemaId: {}, SojSession: {}", value.getSchemaId(), genericRecord);

    return null;
  }


}
