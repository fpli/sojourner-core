package com.ebay.sojourner.distributor.function;

import com.ebay.sojourner.common.model.RawSojSessionWrapper;
import com.ebay.sojourner.common.model.SojSession;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaDeserializer;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaSerializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaDeserializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaSerializer;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class SessionEnhanceMapFunction extends
    RichMapFunction<RawSojSessionWrapper, RawSojSessionWrapper> {

  private transient KafkaDeserializer<SojSession> deserializer;
  private transient KafkaSerializer<SojSession> serializer;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.deserializer = new AvroKafkaDeserializer<>(SojSession.class);
    this.serializer = new AvroKafkaSerializer<>(SojSession.getClassSchema());
  }

  @Override
  public RawSojSessionWrapper map(RawSojSessionWrapper rawSojSessionWrapper) throws Exception {

    SojSession sojSession = deserializer.decodeValue(rawSojSessionWrapper.getValue());
    sojSession.setStreamId(sojSession.getGuid());
    rawSojSessionWrapper.setValue(serializer.encodeValue(sojSession));
    return rawSojSessionWrapper;
  }
}
