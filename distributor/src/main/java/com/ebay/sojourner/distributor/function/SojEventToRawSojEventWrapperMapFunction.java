package com.ebay.sojourner.distributor.function;

import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaSerializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaSerializer;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class SojEventToRawSojEventWrapperMapFunction
    extends RichMapFunction<SojEvent, RawSojEventWrapper> {

  private transient KafkaSerializer<SojEvent> serializer;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    serializer = new AvroKafkaSerializer<>(SojEvent.getClassSchema());
  }

  @Override
  public RawSojEventWrapper map(SojEvent event) throws Exception {
    byte[] payloads = serializer.encodeValue(event);
    return new RawSojEventWrapper(event.getGuid(), event.getPageId(), null, payloads);
  }
}
