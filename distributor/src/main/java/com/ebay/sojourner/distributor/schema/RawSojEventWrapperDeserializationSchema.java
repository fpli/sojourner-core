package com.ebay.sojourner.distributor.schema;

import com.ebay.sojourner.common.model.RawSojEventWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Slf4j
public class RawSojEventWrapperDeserializationSchema
    implements KafkaDeserializationSchema<RawSojEventWrapper> {

  @Override
  public boolean isEndOfStream(RawSojEventWrapper nextElement) {
    return false;
  }

  @Override
  public RawSojEventWrapper deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
    String k = new String(record.key());
    String[] str = k.split(",");
    if (str.length < 2) {
      log.error("Error when deserialize SojEvent with key: {}", k);
      return null;
    }

    int pageId = 0;
    try {
      pageId = Integer.parseInt(str[1]);
    } catch (Exception e) {
      log.error("Cannot parse pageId from message key: {}", k);
      return null;
    }
    return new RawSojEventWrapper(str[0], pageId, null, record.value());
  }

  @Override
  public TypeInformation<RawSojEventWrapper> getProducedType() {
    return TypeInformation.of(RawSojEventWrapper.class);
  }

}
